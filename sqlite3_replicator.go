package replicator

import (
	"bytes"
	"context"
	"crypto/rand"
	"database/sql"
	"database/sql/driver"
	"encoding/base64"
	"fmt"
	"io/ioutil"
	"log"
	"mime/multipart"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/Elbandi/gsync"

	"github.com/CovenantSQL/go-sqlite3-encrypt"

	natsd "github.com/nats-io/gnatsd/server"
	stand "github.com/nats-io/nats-streaming-server/server"
	"github.com/nats-io/nats-streaming-server/stores"
	"github.com/nats-io/stan.go"
	"github.com/ugorji/go/codec"
)

//node configurations
const (
	Primary = iota
	Secondary
	Backup
)

//cbor codec handler
var ch codec.CborHandle

//ackWait is the time the server should wait before resending a message
var ackWait = time.Second * 3

//server ports
var (
	natsPort = 4222
	fsPort   = 6262
)

//dbUpdate represents a single INSERT, UPDATE, or DELETE
type dbUpdate struct {
	QueryStr string
	Args     []interface{}
}

//Connector represents a fixed configuration for an SQLite database
type Connector struct {
	dsn    string
	driver driver.Driver
	errLog *log.Logger
	dir    string
	path   string
	user   *url.Userinfo
	fsHost string
}

//NewConnector returns a pre-configured driver connection
func NewConnector(hostname, dir, dbPath string, user *url.Userinfo, cluster, alias, channel, encryptionKey string, mode int, errLog *log.Logger) (*Connector, error) {
	//setup nats url
	natsURL := url.URL{
		Scheme: "nats",
		User:   user,
		Host:   fmt.Sprintf("%s:%d", hostname, natsPort),
	}

	//declare streaming connection
	var sc stan.Conn

	//connect to the streaming server
	var err error
	sc, err = stan.Connect(cluster, alias, stan.NatsURL(natsURL.String()), stan.SetConnectionLostHandler(func(conn stan.Conn, err error) {
		panic(err)
	}))
	if err != nil {
		return nil, err
	}

	//setup database options
	opts := url.Values{}
	if encryptionKey != "" { //encryption
		opts.Set("_crypto_key", encryptionKey)
	}

	if mode == Primary { //access mode
		opts.Set("mode", "rw")
	} else {
		opts.Set("mode", "ro")
	}

	//normalize the path
	path := filepath.Join(filepath.FromSlash(dir), dbPath)

	//setup DSN
	dsn := url.URL{
		Scheme:   "file",
		Opaque:   path,
		RawQuery: opts.Encode(),
	}

	//setup logger
	if errLog == nil {
		errLog = log.New(log.Writer(), "", log.LstdFlags)
	}
	errLog.SetPrefix("node ")

	//contruct replicator
	c := Connector{
		dsn:    dsn.String(),
		errLog: errLog,
		dir:    dir,
		path:   dbPath,
		user:   user,
		fsHost: fmt.Sprintf("%s:%d", hostname, fsPort),
	}

	//setup the driver based on the mode
	if mode == Primary {
		c.driver = &sqlite3.SQLiteDriver{
			ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				conn.RegisterUpdateHook(func(op int, dbName, tableName string, rowid int64) {
					var updateOp dbUpdate

					if op == sqlite3.SQLITE_DELETE { //delete operation
						//define the update operation
						updateOp.QueryStr = fmt.Sprintf("DELETE FROM %s.%s WHERE ROWID = ?", dbName, tableName)
						updateOp.Args = []interface{}{rowid}
					} else if op == sqlite3.SQLITE_INSERT || op == sqlite3.SQLITE_UPDATE {
						//get updated row
						queryStr := fmt.Sprintf("SELECT * FROM %s.%s WHERE ROWID = ?", dbName, tableName)
						rows, err := conn.Query(queryStr, []driver.Value{rowid})
						if err != nil {
							c.errLog.Println(fmt.Errorf("Failed to retrieve updated row: %w", err))
							return
						}
						defer rows.Close()

						//get values
						values := make([]driver.Value, len(rows.Columns()))
						if err := rows.Next(values); err != nil {
							c.errLog.Println(fmt.Errorf("Failed to read updated row: %w", err))
							return
						}

						//append the arguments
						updateOp.Args = make([]interface{}, len(rows.Columns()))
						for i, v := range values {
							updateOp.Args[i] = v
						}

						//define the query
						if op == sqlite3.SQLITE_INSERT {
							updateOp.QueryStr = fmt.Sprintf("INSERT INTO %s.%s VALUES(%s?)", dbName, tableName, strings.Repeat("?,", len(rows.Columns())-1))
						} else {
							updateOp.QueryStr = fmt.Sprintf("UPDATE %s.%s SET(%s) = (%s?) WHERE ROWID = ?", dbName, tableName,
								strings.Join(rows.Columns(), ","), strings.Repeat("?,", len(rows.Columns())-1))
							updateOp.Args = append(updateOp.Args, rowid)
						}

						//encode change as cbor
						var buf []byte
						if err := codec.NewEncoderBytes(&buf, &ch).Encode(updateOp); err != nil {
							errLog.Panicln(err)
						}

						//send change to the log channel
						if err := sc.Publish(channel, buf); err != nil {
							errLog.Panicln(err)
						}
					}
				})

				return nil
			},
		}
	} else {
		c.driver = &sqlite3.SQLiteDriver{}

		//adjust options to ensure we have a rw mode
		opts.Set("mode", "rw")
		pdsn := url.URL{
			Scheme:   dsn.Scheme,
			Opaque:   dsn.Opaque,
			RawQuery: opts.Encode(),
		}

		//open a private connection pool
		rdb, err := sql.Open("sqlite3", pdsn.String())
		if err != nil {
			return nil, err
		}

		//subscribe for update messages
		sc.Subscribe(channel, func(m *stan.Msg) {
			//decode the update operation
			var updateOp dbUpdate
			if err := codec.NewDecoderBytes(m.Data, &ch).Decode(&updateOp); err != nil {
				c.errLog.Println(fmt.Errorf("Failed to decode message: %w", err))
				return
			}

			//update the database
			if _, err := rdb.Exec(updateOp.QueryStr, updateOp.Args...); err != nil {
				c.errLog.Println(err)

				//download the database file
				if err := c.syncDB(); err != nil {
					c.errLog.Println(fmt.Errorf("Failed to download database: %w", err))
					return
				}

				m.Ack()
				return
			}

			m.Ack()
		}, stan.SetManualAckMode(), stan.MaxInflight(1), stan.DurableName(alias), stan.AckWait(ackWait))
	}

	//clean up
	runtime.SetFinalizer(&c, func(_c *Connector) {
		sc.Close()
		sc.NatsConn().Close()
	})

	return &c, nil
}

//syncDB retrieves the database file from the main server
func (c *Connector) syncDB() error {
	ctx := context.Background()

	//define file path
	path := filepath.Join(filepath.FromSlash(c.dir), c.path)

	//open the basis file
	basis, err := os.Open(path)
	if err != nil {
		c.errLog.Println(err)
	}
	defer basis.Close()

	//compute the signature
	sigs, err := gsync.Signatures(ctx, basis, nil)
	if err != nil {
		return err
	}

	//stream signature
	buf := &bytes.Buffer{}
	body := multipart.NewWriter(buf)
	go func() {
		defer body.Close()
		for sig := range sigs {
			//create new part
			w, err := body.CreatePart(nil)
			if err != nil {
				c.errLog.Println(err)
				return
			}

			//encode the signature
			if err := codec.NewEncoder(w, &ch).Encode(sig); err != nil {
				c.errLog.Println(err)
				return
			}
		}
	}()

	//construct URI
	uri := &url.URL{
		Scheme: "https",
		User:   c.user,
		Host:   c.fsHost,
		Path:   c.path,
	}

	//construct request
	req, err := http.NewRequest(http.MethodGet, uri.String(), buf)
	if err != nil {
		return err
	}

	//set content-type
	req.Header.Set("Content-Type", fmt.Sprintf("multipart/byterange; boundary=%s", body.Boundary()))

	//open the destination file
	out, err := os.Create(path + ".tmp")
	if err != nil {
		return err
	}
	defer out.Close()

	//retrieve the delta
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		respStr, _ := ioutil.ReadAll(resp.Body)
		return fmt.Errorf(string(respStr))
	}

	//get boudary
	_contentType := strings.SplitN(resp.Header.Get("Content-Type"), "=", 2)
	if len(_contentType) < 2 {
		return fmt.Errorf("invalid content-type")
	}
	boundary := _contentType[1]

	//stream the response
	ops := make(chan gsync.BlockOperation)
	r := multipart.NewReader(resp.Body, boundary)

	go func() {
		defer close(ops)
		for {
			//get next part
			part, err := r.NextPart()
			if err != nil {
				break
			}

			//decode operation
			var op gsync.BlockOperation
			if err := codec.NewDecoder(part, &ch).Decode(&op); err != nil {
				return
			}

			log.Printf("updated block: %d", op.Index)

			//channel operation
			ops <- op
		}
	}()

	//apply delta
	if err := gsync.Apply(ctx, out, basis, ops); err != nil {
		return err
	}

	//remove the old basis file and replace it with the newly synchronized version
	if err := os.Remove(path); err != nil {
		c.errLog.Println(err)
	}

	if err := os.Rename(path+".tmp", path); err != nil {
		return err
	}

	return nil
}

//Connect returns a connection to the SQLite database
func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	return c.driver.Open(c.dsn)
}

// Driver returns the underlying SQLiteDriver
func (c *Connector) Driver() driver.Driver {
	return c.driver
}

//ReplicationService implements the necessary backend servers to facilitate replication
type ReplicationService struct {
	natsServer      *natsd.Server
	streamingServer *stand.StanServer
	fileServer      *http.Server
	errLog          *log.Logger
	auth            func(username, password string) bool
	internalToken   string
}

//StartReplicationService configures and runs the server instances that facilitate the replication
func StartReplicationService(cluster, logDir, fsDir string, maxBytes int64, auth func(string, string) bool,
	encryptionkey []byte, cert, key string, errLog *log.Logger) (*ReplicationService, error) {
	//setup logger
	if errLog == nil {
		errLog = log.New(log.Writer(), "", log.LstdFlags)
	}
	errLog.SetPrefix("server ")

	//get random bytes
	buf := make([]byte, 128)
	if _, err := rand.Read(buf); err != nil {
		return nil, err
	}

	//define replication service
	s := ReplicationService{
		auth:          auth,
		internalToken: base64.StdEncoding.EncodeToString(buf),
	}

	//get default options
	opts := stand.GetDefaultOptions()
	nopts := &natsd.Options{}

	//setup server options
	nopts.Port = natsPort
	nopts.MaxPayload = (1024 * 1024 * 2) //2MB payload size
	nopts.CustomClientAuthentication = &s
	opts.ID = cluster
	opts.StoreType = stores.TypeFile
	opts.FilestoreDir = logDir
	opts.FileStoreOpts.DoSync = true
	opts.MaxBytes = maxBytes

	//setup file server options
	s.fileServer = &http.Server{
		Addr: fmt.Sprintf(":%d", fsPort),
		Handler: s.authHandler(func(res http.ResponseWriter, req *http.Request) {
			//get boudary
			_contentType := strings.SplitN(req.Header.Get("Content-Type"), "=", 2)
			if len(_contentType) < 2 {
				http.Error(res, "", http.StatusNotAcceptable)
				return
			}
			boundary := _contentType[1]

			//stream signature
			r := multipart.NewReader(req.Body, boundary)
			sigs := make(chan gsync.BlockSignature)

			go func() {
				defer close(sigs)
				for {
					//get next part
					part, err := r.NextPart()
					if err != nil {
						break
					}

					//decode signature
					var sig gsync.BlockSignature
					if err := codec.NewDecoder(part, &ch).Decode(&sig); err != nil {
						s.errLog.Println(err)
						return
					}

					//channel signature
					sigs <- sig
				}
			}()

			//build lookup table
			lookupTable, err := gsync.LookUpTable(req.Context(), sigs)

			//open source file
			in, err := os.Open(filepath.Join(fsDir, req.URL.Path))
			if err != nil {
				http.Error(res, "error opening source file", http.StatusInternalServerError)
				return
			}

			//compute difference
			ops, err := gsync.Sync(req.Context(), in, nil, nil, lookupTable)
			if err != nil {
				res.WriteHeader(http.StatusBadRequest)
				return
			}

			//stream response
			resp := multipart.NewWriter(res)
			res.Header().Set("Content-Type", fmt.Sprintf("multipart/byterange; boundary=%s", resp.Boundary()))
			for op := range ops {
				//create new part
				w, err := resp.CreatePart(nil)
				if err != nil {
					http.Error(res, "error structuring response", http.StatusInternalServerError)
					return
				}

				//encode data
				if err := codec.NewEncoder(w, &ch).Encode(op); err != nil {
					http.Error(res, "error encoding response", http.StatusInternalServerError)
					return
				}
			}
			resp.Close()
		}),
	}

	//determine if we should enable storage encryption
	if encryptionkey != nil {
		opts.Encrypt = true
		opts.EncryptionCipher = "AES"
		opts.EncryptionKey = encryptionkey
	}

	//determine if we should enable TLS
	if cert != "" && key != "" {
		//nats server TLS
		nopts.TLS = true
		nopts.TLSCert = cert
		nopts.TLSKey = key

		//start file server
		go func() {
			errLog.Panicln(s.fileServer.ListenAndServeTLS(cert, key))
		}()
	} else {
		//start file server
		go func() {
			errLog.Panicln(s.fileServer.ListenAndServe())
		}()
	}

	//start core server
	var err error
	s.natsServer, err = natsd.NewServer(nopts)
	if err != nil {
		return nil, err
	}
	go s.natsServer.Start()

	if ok := s.natsServer.ReadyForConnections(time.Second * 5); !ok {
		return nil, fmt.Errorf("Could not start core nats server")
	}

	//start streaming server
	snopts := stand.NewNATSOptions()
	snopts.Authorization = s.internalToken
	opts.NATSServerURL = s.natsServer.ClientURL()
	if s.streamingServer, err = stand.RunServerWithOpts(opts, snopts); err != nil {
		return nil, err
	}

	return &s, nil
}

//authHandler checks the authentication information for the file server before serving a request on another handler
func (s *ReplicationService) authHandler(next http.HandlerFunc) http.Handler {
	return http.HandlerFunc(func(res http.ResponseWriter, req *http.Request) {
		//get user info
		usr, pwd, ok := req.BasicAuth()
		if !ok {
			http.Error(res, "Authentication Required", http.StatusUnauthorized)
			return
		}

		//check auth
		if s.auth != nil {
			if ok := s.auth(usr, pwd); !ok {
				http.Error(res, "Not Authorized", http.StatusUnauthorized)
				return
			}
		}

		//hand over the request
		next.ServeHTTP(res, req)
	})
}

//Check allows the replication server to acts as a custom nats authenticator
func (s *ReplicationService) Check(c natsd.ClientAuthentication) bool {
	//get client details
	clientOpts := c.GetOpts()

	//make exception for the streaming server
	if clientOpts.Authorization == s.internalToken {
		return true
	}

	//authenticate user
	if s.auth != nil {
		return s.auth(clientOpts.Username, clientOpts.Password)
	}

	return true
}

//Shutdown shuts down the active server
func (s *ReplicationService) Shutdown() error {
	s.natsServer.Shutdown()
	s.streamingServer.Shutdown()
	return s.fileServer.Close()
}

func init() {
	ch.WriterBufferSize = 1 * 1024
	ch.ReaderBufferSize = 1 * 1024
}
