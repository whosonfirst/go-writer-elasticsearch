package elasticsearch

import (
	"context"
	"fmt"
	es "github.com/elastic/go-elasticsearch/v7"
	esapi "github.com/elastic/go-elasticsearch/v7/esapi"
	wof_writer "github.com/whosonfirst/go-writer"
	"io"
	"net/url"
	"strconv"
)

type ElasticsearchWriter struct {
	wof_writer.Writer
	client *es.Client
	index  string
}

func init() {

	ctx := context.Background()
	err := wof_writer.RegisterWriter(ctx, "elasticsearch", NewElasticsearchWriter)

	if err != nil {
		panic(err)
	}
}

func NewElasticsearchWriter(ctx context.Context, uri string) (wof_writer.Writer, error) {

	u, err := url.Parse(uri)

	if err != nil {
		return nil, err
	}

	host := u.Host
	index := u.Path

	port := 9200

	q := u.Query()

	str_port := q.Get("port")

	if str_port != "" {

		p, err := strconv.Atoi(str_port)

		if err != nil {
			return nil, err
		}

		port = p
	}

	var es_endpoint string

	switch port {
	case 443:
		es_endpoint = fmt.Sprintf("https://%s", host)
	default:
		es_endpoint = fmt.Sprintf("http://%s:%d", host, port)
	}

	es_cfg := es.Config{
		Addresses: []string{es_endpoint},
	}

	client, err := es.NewClient(es_cfg)

	if err != nil {
		return nil, err
	}

	wr := &ElasticsearchWriter{
		client: client,
		index:  index,
	}

	return wr, nil
}

func (wr *ElasticsearchWriter) Write(ctx context.Context, uri string, fh io.ReadSeeker) (int64, error) {

	req := esapi.IndexRequest{
		Index:      wr.index,
		DocumentID: uri,
		Body:       fh,
		Refresh:    "true",
	}

	_, err := req.Do(ctx, wr.client)

	if err != nil {
		return 0, err
	}

	return 0, nil
}

func (wr *ElasticsearchWriter) WriterURI(ctx context.Context, uri string) string {
	return uri
}
