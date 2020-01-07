package replicator

import (
	"context"
	"database/sql/driver"
	"fmt"
	"net/url"
	"runtime"
	"strings"

	"github.com/CovenantSQL/go-sqlite3-encrypt"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/stan.go"
	"github.com/ugorji/go/codec"
)

//node configurations
const (
	Primary = iota
	Secondary
)

//cbor codec handler
var ch codec.CborHandle

//dbUpdate represents a single INSERT, UPDATE, or DELETE
type dbUpdate struct {
	QueryStr string
	Args     []interface{}
}

//Connector represents a fixed configuration for an SQLite database
type Connector struct {
	dsn    string
	driver driver.Driver
}

//NewConnector returns a pre-configured driver connection
func NewConnector(dbPath, encryptionKey, saddr, cluster, alias, channel string, mode int, cacert string) (*Connector, error) {
	//setup database specific options
	opts := url.Values{}
	//TODO: possibly implement authentication

	if encryptionKey != "" {
		opts.Set("_crypto_key", encryptionKey)
	}

	if mode == Primary {
		opts.Set("mode", "rw")
	} else {
		opts.Set("mode", "ro")
	}

	//setup dsn
	dsn := url.URL{
		Path:     dbPath,
		RawQuery: opts.Encode(),
	}

	//connection to the core nats server
	nc, err := nats.Connect(saddr, nats.RootCAs(cacert))
	if err != nil {
		return nil, err
	}

	//connect to the streaming server
	sc, err := stan.Connect(cluster, alias, stan.NatsConn(nc), stan.SetConnectionLostHandler(func(conn stan.Conn, err error) {
		panic(err)
	}))
	if err != nil {
		nc.Close()
		return nil, err
	}

	//contruct replicator
	r := Connector{
		dsn: dsn.String(),
	}

	//setup the connector based on the mode
	if mode == Primary {
		r.driver = &sqlite3.SQLiteDriver{
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
							return
						}
						defer rows.Close()

						//get values
						values := make([]driver.Value, len(rows.Columns()))
						if err := rows.Next(values); err != nil {
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
							panic(err)
						}

						//send change to the log channel
						if err := sc.Publish(channel, buf); err != nil {
							panic(err)
						}
					}
				})

				return nil
			},
		}
	} else {
		var s3Conn *sqlite3.SQLiteConn
		r.driver = &sqlite3.SQLiteDriver{
			ConnectHook: func(conn *sqlite3.SQLiteConn) error {
				s3Conn = conn
				return nil
			},
		}

		sc.Subscribe(channel, func(m *stan.Msg) {
			var updateOp dbUpdate
			if err := codec.NewDecoderBytes(m.Data, &ch).Decode(&updateOp); err != nil {
				return
			}
			args := make([]driver.Value, len(updateOp.Args))
			for i, v := range updateOp.Args {
				args[i] = v
			}

			if _, err := s3Conn.Exec(updateOp.QueryStr, args); err != nil {
				return
			}
			m.Ack()
		}, stan.SetManualAckMode(), stan.MaxInflight(1), stan.DurableName(alias))
	}

	//clean up
	runtime.SetFinalizer(&r, func(_r *Connector) {
		sc.Close()
		sc.NatsConn().Close()
	})

	return &r, nil
}

//Connect returns a connection to the SQLite database
func (r *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	return r.driver.Open(r.dsn)
}

// Driver returns the underlying SQLiteDriver
func (r *Connector) Driver() driver.Driver {
	return r.driver
}
