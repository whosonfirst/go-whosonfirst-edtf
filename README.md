# go-whosonfirst-edtf

Go package for working with Extended DateTime Format (EDTF) strings in Who's On First documents.

## Tools

```
$> make cli
go build -mod vendor -o bin/find-invalid cmd/find-invalid/main.go
```

### find-invalid

```
> ./bin/find-invalid -h
Usage of ./bin/find-invalid:
  -include-key
    	Include edtf: property of relevant Who's On First record in output. (default true)
  -include-path
    	Include path of relevant Who's On First record in output. (default true)
  -indexer-uri string
    	A valid whosonfirst/go-whosonfirst-index URI. (default "repo://")
```

For example:

```
$> ./bin/find-invalid -include-path=false /usr/local/data/whosonfirst-data-admin-ca | sort | uniq
edtf:cessation,open
edtf:cessation,uuuu
edtf:inception,uuuu
```

## See also

* https://github.com/whosonfirst/go-edtf
* https://github.com/whosonfirst/go-whosonfirst-index