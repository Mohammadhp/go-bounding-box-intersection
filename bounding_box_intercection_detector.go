package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

const (
	username = "root"
	password = "root"
	hostname = "127.0.0.1:3306"
	dbname   = "fanap"
)

type InputQuery struct {
	Main  Rectangle
	Input []Rectangle
}

type Rectangle struct {
	X      int
	Y      int
	Width  int
	Height int
	Time   string
}

type Coordinate struct {
	X int
	Y int
}

type RectangleDiagonalCoordinates struct {
	TopLeft     Coordinate
	BottomRight Coordinate
}

func getRectangleDiagonalCoordinates(rectangle Rectangle) *RectangleDiagonalCoordinates {
	var rectangleDiagonalCoordinates RectangleDiagonalCoordinates
	rectangleDiagonalCoordinates.TopLeft = Coordinate{X: rectangle.X, Y: rectangle.Y}
	rectangleDiagonalCoordinates.BottomRight = Coordinate{X: rectangle.X + rectangle.Width, Y: rectangle.Y + rectangle.Height}
	return &rectangleDiagonalCoordinates

}

func isRectanglesOverlap(firstRectangle RectangleDiagonalCoordinates, secondRectangle RectangleDiagonalCoordinates) bool {
	var l1, r1, l2, r2 Coordinate = firstRectangle.TopLeft, firstRectangle.BottomRight, secondRectangle.TopLeft, secondRectangle.BottomRight
	if l1.X == r1.X || l1.Y == r1.Y || l2.X == r2.X || l2.Y == r2.Y {
		return false
	}
	if l1.X >= r2.X || l2.X >= r1.X {
		return false
	}
	if r1.Y <= l2.Y || r2.Y <= l1.Y {
		return false
	}
	return true
}

func getOverlappingRectangles(inputQuery InputQuery) []Rectangle {
	overlappingRectangles := []Rectangle{}
	mainDiagonalCoordinates := getRectangleDiagonalCoordinates(inputQuery.Main)
	for _, elem := range inputQuery.Input {
		elemDiagonalCoordinates := getRectangleDiagonalCoordinates(elem)
		if isRectanglesOverlap(*mainDiagonalCoordinates, *elemDiagonalCoordinates) {
			elem.Time = time.Now().Format("01-02-2006 15:04:05")
			overlappingRectangles = append(overlappingRectangles, elem)
		}
	}
	return overlappingRectangles
}

func saveOverlappingRectangles(db *sql.DB, inputQuery InputQuery) {
	err := createRectangleTable(db)
	if err != nil {
		log.Printf("Create rectangle table failed with error %s", err)
		return
	}
	overlappingRectangles := getOverlappingRectangles(inputQuery)
	multipleInsert(db, overlappingRectangles)
}

func dsn(dbName string) string {
	return fmt.Sprintf("%s:%s@tcp(%s)/%s", username, password, hostname, dbName)
}

func dbConnection() (*sql.DB, error) {
	db, err := sql.Open("mysql", dsn(""))
	if err != nil {
		log.Printf("Error %s when opening DB", err)
		return nil, err
	}

	ctx, cancelfunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelfunc()
	res, err := db.ExecContext(ctx, "CREATE DATABASE IF NOT EXISTS "+dbname)
	if err != nil {
		log.Printf("Error %s when creating DB", err)
		return nil, err
	}
	no, err := res.RowsAffected()
	if err != nil {
		log.Printf("Error %s when fetching rows", err)
		return nil, err
	}
	log.Printf("rows affected %d\n", no)

	db.Close()
	db, err = sql.Open("mysql", dsn(dbname))
	if err != nil {
		log.Printf("Error %s when opening DB", err)
		return nil, err
	}

	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(20)

	ctx, cancelfunc = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelfunc()
	err = db.PingContext(ctx)
	if err != nil {
		log.Printf("Errors %s pinging DB", err)
		return nil, err
	}
	log.Printf("Connected to DB %s successfully\n", dbname)
	return db, nil
}

func createRectangleTable(db *sql.DB) error {
	query := `CREATE TABLE IF NOT EXISTS rectangle(rectangle_id int primary key auto_increment, x int, y int, width int, height int, time text)`
	ctx, cancelfunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelfunc()
	res, err := db.ExecContext(ctx, query)
	if err != nil {
		log.Printf("Error %s when creating rectangle table", err)
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		log.Printf("Error %s when getting rows affected", err)
		return err
	}
	log.Printf("Rows affected when creating table: %d\n\n", rows)
	return nil
}

func multipleInsert(db *sql.DB, rectangles []Rectangle) error {
	query := "INSERT INTO rectangle(x, y, width, height, time) VALUES "
	var inserts []string
	var params []interface{}
	for _, rectangle := range rectangles {
		inserts = append(inserts, "(?, ?, ?, ?, ?)")
		params = append(params, rectangle.X, rectangle.Y, rectangle.Width, rectangle.Height, rectangle.Time)
	}
	queryVals := strings.Join(inserts, ",")
	query = query + queryVals
	log.Println("query is", query)
	ctx, cancelfunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelfunc()
	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		log.Printf("Error %s when preparing SQL statement", err)
		return err
	}
	defer stmt.Close()
	res, err := stmt.ExecContext(ctx, params...)
	if err != nil {
		log.Printf("Error %s when inserting row into rectangle table", err)
		return err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		log.Printf("Error %s when finding rows affected", err)
		return err
	}
	log.Printf("%d rectangles created simulatneously\n\n", rows)
	return nil
}

func selectRectangles(db *sql.DB) ([]Rectangle, error) {
	log.Printf("Getting Rectangles\n\n")
	query := `select x, y, width, height, time from rectangle`
	ctx, cancelfunc := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelfunc()
	stmt, err := db.PrepareContext(ctx, query)
	if err != nil {
		log.Printf("Error %s when preparing SQL statement", err)
		return []Rectangle{}, err
	}
	defer stmt.Close()
	rows, err := stmt.QueryContext(ctx)
	if err != nil {
		return []Rectangle{}, err
	}
	defer rows.Close()
	var rectangles = []Rectangle{}
	for rows.Next() {
		var rectangle Rectangle
		if err := rows.Scan(&rectangle.X, &rectangle.Y, &rectangle.Width, &rectangle.Height, &rectangle.Time); err != nil {
			return []Rectangle{}, err
		}
		rectangles = append(rectangles, rectangle)
	}
	if err := rows.Err(); err != nil {
		return []Rectangle{}, err
	}
	return rectangles, nil
}

func handleRequest(w http.ResponseWriter, req *http.Request) {
	db, err := dbConnection()
	if err != nil {
		log.Printf("Error %s when getting db connection", err)
		return
	}
	defer db.Close()

	switch req.Method {

	case "GET":
		savedRectangles, _ := selectRectangles(db)
		json.NewEncoder(w).Encode(savedRectangles)

	case "POST":
		decoder := json.NewDecoder(req.Body)
		var inputQuery InputQuery
		err := decoder.Decode(&inputQuery)
		if err != nil {
			panic(err)
		}
		saveOverlappingRectangles(db, inputQuery)
	default:
		fmt.Fprintf(w, "Sorry, only GET and POST methods are supported.")
	}
}

func main() {
	http.HandleFunc("/", handleRequest)
	http.ListenAndServe(":8090", nil)
}
