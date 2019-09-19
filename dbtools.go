package main

import (
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"github.com/davecgh/go-spew/spew"
	_ "github.com/davecgh/go-spew/spew"
	_ "github.com/mattn/go-sqlite3"
)

const TABLE_NAME = "messages"

// user code - the only thing the library user needs to write
type Message struct {
	ID    *int
	Title *string
	Body  *string
}

func (m *Message) toTableMap(db *sql.DB) *TableMap {
	tm := NewTableMap(db, TABLE_NAME)
	tm.IntCol("id", FromInt(m.ID))
	tm.StringCol("title", FromString(m.Title))
	tm.StringCol("body", FromString(m.Body))
	return tm
}

// end user code

func main() {
	db, err := sql.Open("sqlite3", "./foo.db")
	checkErr(err)
	defer db.Close()

	prepareDB(db)

	id := 1
	title := "My Title"
	body := "My Body"
	msg := Message{ID: &id, Title: &title, Body: &body}

	tm := msg.toTableMap(db)
	tm.Print()

	_, err = tm.Create()
	checkErr(err)
	fmt.Println("-----------")

	msg = Message{ID: &id}
	tm = msg.toTableMap(db)

	// Setting the struct from the database is still pretty clunky. The
	// alternatives are:
	//
	// 1. store pointers from the struct in a closure, then set
	// value of the pointer; the problem with this is if the pointers are nil,
	// you can't re-set the underlying value and still have it associated with
	// the struct.
	//
	// 2. the method below, where I've at least abstracted away the boilerplate
	// and the user just provides a function to process sql.Rows
	//
	// 3. use reflection. the performance penalty probably doesn't matter, and
	// we can store the correct setters in a closure to prevent bugs. But it's
	// still basically "unsafe" code.
	//
	// example of setting a field with reflection
	// val := reflect.ValueOf(&n)
	// (val.Elem()).FieldByName("title").SetString("My Title")
	//
	// 4. I'm sure there's also an approach using type assertions (rows.Scan into
	// an appropriately-sized array of interface{}), and like reflection we could
	// store the type assertion in the appropriately-typed closure. But then we
	// still have the null pointer problem.

	var fetchedMessages []Message
	err = tm.Find(func(rows *sql.Rows) error {
		var id int
		var title string
		var body string

		err = rows.Scan(&id, &title, &body)
		if err != nil {
			return err
		}

		fetchedMessages = append(
			fetchedMessages,
			Message{ID: &id, Title: &title, Body: &body})

		return nil
	})
	checkErr(err)
	spew.Dump(fetchedMessages)
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

// library code

type TableMap struct {
	DB         *sql.DB
	TableName  string
	Fields     map[string]TableMapField
	fieldOrder []string
}

func NewTableMap(db *sql.DB, tableName string) *TableMap {
	tm := TableMap{DB: db, TableName: tableName, Fields: make(map[string]TableMapField)}
	return &tm
}

func (f *TableMap) Print() {
	fields := f.Fields
	for colname, slfield := range fields {
		v := slfield.Val()

		var output string
		if v.Valid {
			output = v.String
		} else {
			output = "<null>"
		}

		fmt.Printf("%s : %s\n", colname, output)
	}
}

func (f *TableMap) CreateSql() (string, []interface{}) {
	cols, placeholders, vals := f.GetFields()

	sql := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)\n",
		f.TableName,
		strings.Join(cols[:], ","),
		strings.Join(placeholders[:], ","))

	return sql, vals
}

func (f *TableMap) Create() (sql.Result, error) {
	sql, vals := f.CreateSql()
	r, err := f.DB.Exec(sql, vals...)
	return r, err
}

func (f *TableMap) FindSql() (string, []interface{}) {
	allcols, _, _ := f.GetFields()
	cols, placeholders, vals := f.GetFieldsWithoutNulls()

	var where []string
	for i, col := range cols {
		cond := col + "=" + placeholders[i]
		where = append(where, cond)
	}

	sql := fmt.Sprintf("SELECT %s FROM %s WHERE %s",
		strings.Join(allcols[:], ","),
		f.TableName,
		strings.Join(where[:], ","))
	return sql, vals
}

func (f *TableMap) Find(parser func(rows *sql.Rows) error) error {
	sql, vals := f.FindSql()

	rows, err := f.DB.Query(sql, vals...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		err := parser(rows)
		if err != nil {
			return err
		}
	}

	return nil
}

func (f *TableMap) GetFieldsWithoutNulls() ([]string, []string, []interface{}) {
	return f.getFieldsHelper(false)
}

func (f *TableMap) GetFields() ([]string, []string, []interface{}) {
	return f.getFieldsHelper(true)
}

func (f *TableMap) getFieldsHelper(inclnull bool) ([]string, []string, []interface{}) {
	var cols []string
	var vals []interface{}

	for _, fieldName := range f.fieldOrder {
		v := f.Fields[fieldName].Val()

		if !v.Valid && !inclnull {
			continue
		}

		cols = append(cols, fieldName)

		if v.Valid {
			vals = append(vals, v.String)
		} else {
			vals = append(vals, "NULL")
		}
	}

	// this will depend on database driver
	var placeholders []string
	for range cols {
		placeholders = append(placeholders, "?")
	}

	return cols, placeholders, vals
}

type TableMapField struct {
	Val TableMapInput
}

type TableMapInput func() sql.NullString

// The ___Col methods associate the given input with a typed DB column and
// ensure it's compatible with that column type. For example:
// - IntCol checks to ensure the given value is a valid integer in SQL.
// - TimeCol (TBD) would run the db function CONVERT on the value (for postgres).

func (f *TableMap) IntCol(name string, input TableMapInput) {
	inputChecked := func() sql.NullString {
		v := input()
		_, err := strconv.Atoi(v.String)
		if err != nil {
			return sql.NullString{String: "", Valid: false}
		}

		if v.Valid {
			return v
		} else {
			return sql.NullString{String: "", Valid: false}
		}
	}
	f.StringCol(name, inputChecked)
}

func (f *TableMap) StringCol(name string, input TableMapInput) {
	m := TableMapField{Val: input}
	f.Fields[name] = m
	f.fieldOrder = append(f.fieldOrder, name)
}

// The From_____ methods basically take the column and converts it into a
// sql.NullString.  We'll do nil-handling later in getFieldsHelper.

func FromString(v *string) TableMapInput {
	return func() sql.NullString {
		if v == nil {
			return sql.NullString{String: "", Valid: false}
		} else {
			return sql.NullString{String: *v, Valid: true}
		}
	}
}

func FromInt(v *int) TableMapInput {
	return func() sql.NullString {
		if v == nil {
			return sql.NullString{String: "", Valid: false}
		} else {
			s := strconv.Itoa(*v)
			return sql.NullString{String: s, Valid: true}
		}
	}
}

// setup / teardown; this should be managed by a separate db migration library

func prepareDB(db *sql.DB) {
	dropstmt := "DROP TABLE IF EXISTS " + TABLE_NAME
	_, err := db.Exec(dropstmt)
	checkErr(err)

	createstmt := "CREATE TABLE " + TABLE_NAME + `(
		id INTEGER PRIMARY_KEY,
		title TEXT,
		body TEXT
	)`
	_, err = db.Exec(createstmt)
	checkErr(err)
}
