// Copyright 2017 Rubrik, Inc.

// +build testserver

package example

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/jinzhu/gorm"
	_ "github.com/jinzhu/gorm/dialects/postgres"

	"rubrik/util/crdbutil"
)

type testClass struct {
	UUID string `gorm:"primary_key"`
	Size int
}

func start(t *testing.T) (*gorm.DB, func()) {
	ts, err := crdbutil.NewTestServer(t)
	if err != nil {
		t.Fatal(err)
	}
	err = ts.Start()
	if err != nil {
		t.Fatal(err)
	}
	url := ts.PGURL()
	if url == nil {
		t.Fatalf("url not found")
	}
	sqlDb, err := sql.Open("postgres", url.String())
	if err != nil {
		t.Fatal(err)
	}
	database := "test"
	if _, err := sqlDb.Exec(fmt.Sprintf("CREATE DATABASE %s", database)); err != nil {
		t.Fatal("schema install failed:", err)
	}
	url.Path = database
	db, err := gorm.Open("postgres", url.String())
	if err != nil {
		t.Fatal(err)
	}
	db.LogMode(false)
	if err := db.Error; err != nil {
		t.Fatal("couldn't create DB rubrik due to error:", err)
	}
	db.DropTableIfExists("test_classes")
	if err := db.CreateTable(&testClass{}).Error; err != nil {
		t.Fatal("couldn't create table test_class due to error:", err)
	}
	return db, func() {
		db.Close()
		ts.Stdout()
	}
}

func TestCreateIfExists(t *testing.T) {
	db, close := start(t)
	defer close()
	if err := db.Create(&testClass{UUID: "try1", Size: 10}).Error; err != nil {
		t.Errorf("Error while inserting first time - %v", err)
	}
	expectedError := "pq: duplicate key value (uuid)=('try1') violates unique constraint \"primary\""
	if err := db.Create(&testClass{UUID: "try1", Size: 10}).Error; err.Error() != expectedError {
		t.Errorf("Expected error :- %s, Got Error :- %s", expectedError, err.Error())
	}
}

func TestRemoveNotExist(t *testing.T) {
	db, close := start(t)
	defer close()
	defer db.Close()
	// expectedError :- nil
	var expectedError error
	if err := db.Delete(testClass{}, "uuid = ?", "try1").Error; err != expectedError {
		t.Errorf("Expected  error :- %v , Got Error :- %v", expectedError, err)
	}
}

func TestFindIfNotExists(t *testing.T) {
	db, close := start(t)
	defer close()
	defer db.Close()
	var test []testClass
	// expectedError :- nil
	var expectedError error
	if err := db.Where("uuid = ?", "try1").Find(&test).Error; err != expectedError {
		t.Errorf("Expected  error :- %v , Got Error :- %v", expectedError, err)
	}
	if len(test) != 0 {
		t.Errorf("Expected  size of test :- %d , Got Size :- %d", 0, len(test))
	}
}

func TestFirstIfNotExists(t *testing.T) {
	db, close := start(t)
	defer close()
	defer db.Close()
	var test testClass
	// expectedError :- nil
	expectedError := "record not found"
	if err := db.Where("uuid = ?", "try1").First(&test).Error; err.Error() != expectedError {
		t.Errorf("Expected  error :- %s , Got Error :- %s", expectedError, err.Error())
	}
}
