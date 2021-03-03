package main

import (
	"encoding/json"
	"encoding/xml"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"context"

	"cloud.google.com/go/firestore"
	firebase "firebase.google.com/go"

	"google.golang.org/api/option"
)

type product struct {
	XMLName xml.Name `xml:"product"`
	ID      string   `json:"id" xml:"code"`
	Amount  int      `json:"amount" xml:"amount"`
}

type order struct {
	XMLName  xml.Name  `xml:"order"`
	UID      string    `xml:"uid"`
	Date     time.Time `xml:"date"`
	Products []product `xml:"products>product"`
}

var pIDFile string

func init() {
	pIDFile, _ = filepath.Abs(filepath.Dir(os.Args[0]))

	pIDFile += "/.pid"
}

func main() {
	if len(os.Args) != 3 {
		fmt.Printf("Usage: %s path/to/orders.xml path/to/key.json\n(got %+v)\n", os.Args[0], os.Args[1:])

		return
	}

	err := safetyCheck(os.Args[1])

	if err != nil {
		panic(err)
	}

	os.WriteFile(pIDFile, []byte(strconv.Itoa(os.Getpid())), 0644)

	defer os.Remove(pIDFile)

	client, err := setupFirestore(os.Args[2])

	if err != nil {
		panic(err)
	}

	documents, err := fetchDocuments(client)

	if err != nil {
		panic(err)
	}

	err = saveOrders(os.Args[1], parseOrders(documents))

	if err != nil {
		panic(err)
	}

	err = deleteDocuments(client, documents)

	if err != nil {
		panic(err)
	}
}

func setupFirestore(credentialsFilename string) (*firestore.Client, error) {
	app, err := firebase.NewApp(context.Background(), nil, option.WithCredentialsFile(credentialsFilename))

	if err != nil {
		return nil, fmt.Errorf("error initializing firebase: %v", err)
	}

	client, err := app.Firestore(context.Background())

	if err != nil {
		return nil, fmt.Errorf("error initializing firestore: %v", err)
	}

	return client, nil
}

func fetchDocuments(client *firestore.Client) ([]*firestore.DocumentSnapshot, error) {
	orderDocuments, err := client.Collection("orders").OrderBy("date", firestore.Desc).Documents(context.Background()).GetAll()

	if err != nil {
		return nil, fmt.Errorf("error retrieving dordersocuments: %v", err)
	}

	return orderDocuments, nil
}

func deleteDocuments(client *firestore.Client, documents []*firestore.DocumentSnapshot) error {
	batch := client.Batch()

	for _, document := range documents {
		batch.Delete(document.Ref)
	}

	_, err := batch.Commit(context.Background())

	if err != nil {
		return fmt.Errorf("error deleting orders: %v", err)
	}

	return nil
}

func parseOrders(documents []*firestore.DocumentSnapshot) []order {
	orders := make([]order, len(documents))

	for i, document := range documents {
		data := document.Data()

		productsStr := data["products"].([]interface{})

		orders[i].Products = make([]product, len(productsStr))

		for j, ps := range productsStr {
			json.Unmarshal([]byte(ps.(string)), &orders[i].Products[j])
		}

		orders[i].UID = data["uid"].(string)

		orders[i].Date = data["date"].(time.Time)
	}

	return orders
}

func saveOrders(filename string, orders []order) error {
	type output struct {
		XMLName xml.Name `xml:"orders"`
		Orders  []order  `xml:"order"`
	}

	data, err := xml.Marshal(output{Orders: orders})

	if err != nil {
		return fmt.Errorf("error converting to xml: %v", err)
	}

	tmpFilename := filename + ".tmp"

	err = os.WriteFile(tmpFilename, data, 0644)

	if err != nil {
		return fmt.Errorf("error writing file %s: %v", tmpFilename, err)
	}

	err = os.Rename(tmpFilename, filename)

	if err != nil {
		return fmt.Errorf("error writing file %s: %v", filename, err)
	}

	return nil
}

func safetyCheck(filename string) error {
	tmpFilename := filename + ".tmp"

	if _, err := os.Stat(tmpFilename); err == nil {
		return fmt.Errorf("%s exists", tmpFilename)
	}

	if _, err := os.Stat(filename); err == nil {
		return fmt.Errorf("%s exists", filename)
	}

	if _, err := os.Stat(pIDFile); err == nil {
		return fmt.Errorf("another proccess is running, check %s", pIDFile)
	}

	return nil
}
