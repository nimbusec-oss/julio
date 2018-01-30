// Package julio enables to use PostgreSQL as storage for a simple JSON based
// event sourcing.
package julio

import (
	"database/sql"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	"github.com/Masterminds/squirrel"
	"github.com/lib/pq"
)

var psql = squirrel.StatementBuilder.PlaceholderFormat(squirrel.Dollar)

const prefix = "julio_notify"

// Julio wraps a PostgreSQL database connection to be used for event
// sourcing. The underlying connection is exposed as DB.
type Julio struct {
	DB         *sql.DB
	dataSource string
}

// Open opens a new database connection.
func Open(dataSource string) (*Julio, error) {
	db, err := sql.Open("postgres", dataSource)
	if err != nil {
		return nil, err
	}

	return &Julio{
		DB:         db,
		dataSource: dataSource,
	}, err
}

// Init initializes a table for event sourcing. It is safe to call
// init for allready initialized tables, but not for existing tables
// with a different structure.
func (j *Julio) Init(table string) error {
	query := `
		CREATE OR REPLACE FUNCTION <PREFIX>_<TABLE>() RETURNS TRIGGER AS $$
			BEGIN
				PERFORM pg_notify('<PREFIX>_<TABLE>', NEW.id::text);
				RETURN NULL;
			END;
		$$ LANGUAGE plpgsql;
		
		CREATE TABLE IF NOT EXISTS <TABLE> (
			id BIGSERIAL PRIMARY KEY,
			data JSONB NOT NULL
		);

		CREATE INDEX IF NOT EXISTS <TABLE>_data_idx
			ON <TABLE>
			USING gin
			(data jsonb_path_ops);

		DROP TRIGGER IF EXISTS <PREFIX> ON <TABLE>;
		CREATE TRIGGER <PREFIX>
			AFTER INSERT ON <TABLE>
			FOR EACH ROW EXECUTE PROCEDURE <PREFIX>_<TABLE>();`
	query = strings.Replace(query, "<TABLE>", table, -1)
	query = strings.Replace(query, "<PREFIX>", prefix, -1)
	_, err := j.DB.Exec(query)
	if err != nil {
		return err
	}

	return nil
}

// Add adds a new event entry to the given table. The payload v is
// json serialized in the database.
func (j *Julio) Add(table string, v interface{}) (int, error) {
	data, err := json.Marshal(v)
	if err != nil {
		return 0, err
	}

	query, args, err := psql.
		Insert(table).
		Columns("data").
		Values(data).
		Suffix("RETURNING id").
		ToSql()
	if err != nil {
		return 0, err
	}

	id := 0
	err = j.DB.QueryRow(query, args...).Scan(&id)
	return id, err
}

// Get fetches all events from table that match the given filter. It
// optionally listes for new events as well.
func (j *Julio) Get(table string, filter Filter) *Rows {
	rows := &Rows{
		C:       make(chan Row, 1024),
		backlog: make(chan Row, 1024),
		done:    make(chan struct{}),

		julio:  j,
		table:  table,
		filter: filter,
	}

	go rows.notifyloop()
	go rows.selectloop()
	return rows
}

// Filter defines the SQL where predicate to filter existing and new
// rows alike.
type Filter struct {
	Sqlizer squirrel.Sqlizer
	Offset  uint64
	Updates bool
	OnlyNew bool // subscribe only to new events, skip the "historic"
}

// Row is a single row or event in the database
type Row struct {
	ID   int
	Data json.RawMessage
}

// Rows is a collection of rows in the database. Both existing and
// new events can be retrieved via the attribute C. If you no longer
// are interested in events close it.
type Rows struct {
	C   chan Row
	Err error

	julio   *Julio
	table   string
	filter  Filter
	backlog chan Row
	done    chan struct{}
}

func (r *Rows) notifyloop() {
	defer close(r.backlog)
	if !r.filter.Updates {
		return
	}

	listener := pq.NewListener(r.julio.dataSource,
		10*time.Second,
		1*time.Minute,
		func(ev pq.ListenerEventType, err error) {})
	err := listener.Listen(prefix + "_" + r.table)
	if err != nil {
		r.Err = err
		return
	}

	defer listener.Close()
	for {
		select {
		case <-r.done:
			return

		case n := <-listener.Notify:
			id, err := strconv.Atoi(n.Extra)
			if err != nil {
				r.Err = err
				return
			}

			query, args, err := psql.
				Select("id", "data").
				From(r.table).
				Where(squirrel.And{
					squirrel.Eq{"id": id},
					r.filter.Sqlizer}).
				OrderBy("id").
				ToSql()
			if err != nil {
				r.Err = err
				return
			}

			rows, err := r.julio.DB.Query(query, args...)
			if err != nil {
				r.Err = err
				return
			}

			defer rows.Close()
			for rows.Next() {
				event := Row{}
				err := rows.Scan(&event.ID, &event.Data)
				if err != nil {
					r.Err = err
					return
				}

				select {
				case r.backlog <- event:
				case <-r.done:
					return
				}

			}

			if rows.Err() != nil {
				r.Err = rows.Err()
				return
			}

		case <-time.After(90 * time.Second):
			go listener.Ping()
		}
	}
}

func (r *Rows) selectloop() {
	defer close(r.C)

	r.query()
	for row := range r.backlog {
		r.C <- row
	}
}

func (r *Rows) query() {
	if r.filter.OnlyNew {
		// skip querying if we are only interested in new events
		return
	}

	query, args, err := psql.
		Select("id", "data").
		From(r.table).
		Where(r.filter.Sqlizer).
		Where(squirrel.Gt{"id": r.filter.Offset}).
		OrderBy("id").
		ToSql()
	if err != nil {
		r.Err = err
		return
	}

	rows, err := r.julio.DB.Query(query, args...)
	if err != nil {
		r.Err = err
		return
	}

	for rows.Next() {
		row := Row{}
		err := rows.Scan(&row.ID, &row.Data)
		if err != nil {
			r.Err = err
			return
		}

		select {
		case r.C <- row:
		case <-r.done:
			return
		}
	}

	rows.Close()
	if rows.Err() != nil {
		r.Err = rows.Err()
		return
	}
}

// Close closes this notify subscription.
func (r *Rows) Close() {
	defer func() { recover() }()
	close(r.done)
}
