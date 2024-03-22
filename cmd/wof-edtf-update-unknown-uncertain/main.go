package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"strings"

	"github.com/tidwall/gjson"
	"github.com/whosonfirst/go-whosonfirst-edtf"
	"github.com/whosonfirst/go-whosonfirst-iterate/v2/emitter"
	"github.com/whosonfirst/go-whosonfirst-iterate/v2/iterator"
	wof_writer "github.com/whosonfirst/go-whosonfirst-writer/v3"
	"github.com/whosonfirst/go-writer/v3"
)

func main() {

	emitter_schemes := strings.Join(emitter.Schemes(), ",")
	iterator_desc := fmt.Sprintf("A valid whosonfirst/go-whosonfirst-iterate/v2 URI. Supported emitter URI schemes are: %s", emitter_schemes)

	iterator_uri := flag.String("iterator-uri", "repo://", iterator_desc)

	writer_uri := flag.String("writer-uri", "null://", "A valid whosonfirst/go-writer URI.")

	flag.Parse()

	uris := flag.Args()

	ctx := context.Background()

	wr, err := writer.NewWriter(ctx, *writer_uri)

	if err != nil {
		log.Fatalf("Failed to create writer for '%s', %v", *writer_uri, err)
	}

	iter_cb := func(ctx context.Context, path string, fh io.ReadSeeker, args ...interface{}) error {

		body, err := io.ReadAll(fh)

		if err != nil {
			return err
		}

		id_rsp := gjson.GetBytes(body, "properties.wof:id")

		if !id_rsp.Exists() {
			return errors.New("missing wof:id property")
		}

		id := id_rsp.Int()

		changed, body, err := edtf.UpdateBytes(body)

		if err != nil {
			return fmt.Errorf("Failed to apply EDTF updates to %d, %w", id, err)
		}

		if !changed {
			return nil
		}

		_, err = wof_writer.WriteBytes(ctx, wr, body)

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
