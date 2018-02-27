package main

import (
	"fmt"
	"time"

	"github.com/go-pg/pg"
	"github.com/google/btree"
)

const dbReadStep = 100000
const myLat = 45.772917
const myLng = 15.976464
const distance = 0.5

type dbChestRecord struct {
	ID int `sql:"id"`
	Did int `sql:"did"`
	Lat float32 `sql:"lat"`
	Lng float32 `sql:"lng"`
}

// A lattitude, contains a BTree of longitutes
type GeoLat struct {
	lat float32
	lngs *btree.BTree
}

func (a GeoLat) Less(b btree.Item) bool {
	return a.lat < b.(GeoLat).lat
}

// A longitude. Contains the DB ID of a chest.
type GeoLng struct {
	lng float32
	id int
}

func (a GeoLng) Less(b btree.Item) bool {
	return a.lng < b.(GeoLng).lng
}

// The tree of lattitudes, each containing a tree of longitudes
type GeoTree struct {
	*btree.BTree
}

func (t GeoTree) AddCoordinate(lat, lng float32, id int) {
	tree_lat := t.Get(GeoLat{lat: lat})
	if tree_lat != nil {
		tree_lat.(GeoLat).lngs.ReplaceOrInsert(GeoLng{lng: lng, id: id})
	} else {
		t.ReplaceOrInsert(GeoLat{lat: lat, lngs: btree.New(2)})
	}
}

func (t GeoTree) FindPoints(from_lat, from_lng, to_lat, to_lng float32) []int {
	result := []int{}

	t.AscendRange(GeoLat{lat: from_lat}, GeoLat{lat: to_lat}, func(il btree.Item) bool {
		l := il.(GeoLat)
		l.lngs.AscendRange(GeoLng{lng: from_lng}, GeoLng{lng: to_lng}, func(ix btree.Item) bool {
			x := ix.(GeoLng)
			result = append(result, x.id)
			return true
		})
		return true
	})

	return result
}

// This is a BTree of GeoLats, which each contain a BTree of GeoLngs
var lats GeoTree

func main() {
	db := pg.Connect(&pg.Options{
		User: "ivoras",
		Database: "ivoras",
	})

	lats = GeoTree{ btree.New(2)}
	
	var cmin, cmax int
	_, err := db.QueryOne(pg.Scan(&cmin, &cmax), "SELECT MIN(id), MAX(id) FROM chest_v2")
	if err != nil {
		panic(err)
	}
	for i := cmin; i <= cmax; i += dbReadStep {
		recs := []dbChestRecord{}
		_, err := db.Query(&recs, "SELECT * FROM chest_v2 WHERE id BETWEEN ? AND ?", i, i+dbReadStep)
		if err != nil {
			panic(err)
		}
		fmt.Println(i)
		for _, rec := range(recs) {
			lats.AddCoordinate(rec.Lat, rec.Lng, rec.ID)
		}
		break
	}

	t1 := time.Now()

	chests := lats.FindPoints(myLat - distance, myLng - distance, myLat + distance, myLng + distance)

	fmt.Println(time.Since(t1), len(chests))
}