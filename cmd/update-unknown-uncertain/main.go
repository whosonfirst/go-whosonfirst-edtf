package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"github.com/sfomuseum/go-edtf"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
	"github.com/whosonfirst/go-whosonfirst-export/v2"
	"github.com/whosonfirst/go-whosonfirst-iterate/emitter"
	"github.com/whosonfirst/go-whosonfirst-iterate/iterator"
	wof_writer "github.com/whosonfirst/go-whosonfirst-writer"
	"github.com/whosonfirst/go-writer"
	"io"
	"log"
	"strings"
)

func main() {

	emitter_schemes := strings.Join(emitter.Schemes(), ",")
	iterator_desc := fmt.Sprintf("A valid whosonfirst/go-whosonfirst-iterate/emitter URI. Supported emitter URI schemes are: %s", emitter_schemes)

	iterator_uri := flag.String("iterator-uri", "repo://", iterator_desc)

	exporter_uri := flag.String("exporter-uri", "whosonfirst://", "A valid whosonfirst/go-whosonfirst-export URI.")
	writer_uri := flag.String("writer-uri", "null://", "A valid whosonfirst/go-writer URI.")

	flag.Parse()

	uris := flag.Args()

	ctx := context.Background()

	ex, err := export.NewExporter(ctx, *exporter_uri)

	if err != nil {
		log.Fatalf("Failed to create exporter for '%s', %v", *exporter_uri, err)
	}

	wr, err := writer.NewWriter(ctx, *writer_uri)

	if err != nil {
		log.Fatalf("Failed to create writer for '%s', %v", *writer_uri, err)
	}

	iter_cb := func(ctx context.Context, fh io.ReadSeeker, args ...interface{}) error {

		path, err := emitter.PathForContext(ctx)

		if err != nil {
			return err
		}

		body, err := io.ReadAll(fh)

		if err != nil {
			return err
		}

		props := gjson.GetBytes(body, "properties")

		if !props.Exists() {
			return nil
		}

		id_rsp := gjson.GetBytes(body, "properties.wof:id")

		if !id_rsp.Exists() {
			return errors.New("Missing wof:id property")
		}

		id := id_rsp.Int()

		changed := false

		for k, v := range props.Map() {

			if !strings.HasPrefix(k, "edtf:") {
				continue
			}

			path := fmt.Sprintf("properties.%s", k)

			var err error

			switch v.String() {
			case "open":

				body, err = sjson.SetBytes(body, path, edtf.OPEN)

				if err != nil {
					return err
				}

				changed = true

			case "uuuu":

				body, err = sjson.SetBytes(body, path, edtf.UNKNOWN)

				if err != nil {
					return err
				}

				changed = true

			default:
				// pass
			}
		}

		if !changed {
			return nil
		}

		new_body, err := ex.Export(ctx, body)

		if err != nil {
			return err
		}

		err = wof_writer.WriteFeatureBytes(ctx, wr, new_body)

		if err != nil {
			return err
		}

		log.Printf("Updated %d (%s)\n", id, path)
		return nil
	}

	iter, err := iterator.NewIterator(ctx, *iterator_uri, iter_cb)

	if err != nil {
		log.Fatal(err)
	}

	err = iter.IterateURIs(ctx, uris...)

	if err != nil {
		log.Fatal(err)
	}
}
