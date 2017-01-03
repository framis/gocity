package lib

import (
	"github.com/algolia/algoliasearch-client-go/algoliasearch"
	"github.com/framis/gocity/model"
	"fmt"
	"github.com/fatih/structs"
)

type Indexer interface {
	Index()
}

type AlgoliaIndexer struct {
	client algoliasearch.Client
	inboundChan chan model.City
	index algoliasearch.Index
}

func NewAlgoliaIndexer(appId string, appSecret string, indexName string, inboundChan chan model.City) *AlgoliaIndexer {
	indexer := AlgoliaIndexer{}
	indexer.client = algoliasearch.NewClient(appId, appSecret)
	indexer.index = indexer.client.InitIndex(indexName)
	indexer.inboundChan = inboundChan

	settings := algoliasearch.Map{
		"searchableAttributes": []string{"Name", "AlternateNames", "Administrative", "Country", "PostalCode"},
		"customRanking": []string{"desc(Population)"},
	}

	_, err := indexer.index.SetSettings(settings)
	if err != nil {
		fmt.Println(err)
	}

	return &indexer
}

func (algolia *AlgoliaIndexer) Index() {
	batchSize := 1000
	i := 0
	objects := make([]algoliasearch.Object, 0)
	for record := range algolia.inboundChan {
		var object algoliasearch.Object
		object = structs.Map(record)
		object["objectID"] = record.GeonameId
		objects = append(objects, object)
		i++
		if i%batchSize == 0 {
			algolia.indexObjects(objects, batchSize, i)
			objects = make([]algoliasearch.Object, 0)
		}
	}

	if len(objects) > 0 {
		algolia.indexObjects(objects, len(objects), i)
	}
}

func (algolia *AlgoliaIndexer) indexObjects(objects []algoliasearch.Object, size int, lastIndex int) {
	_, err := algolia.index.AddObjects(objects)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Printf("Indexed %d objects to Algolia, last index %d\n", size, lastIndex)
}
