package sqload

import (
	"context"
	"encoding/csv"
	"io/ioutil"
	"os"

	"rubrik/util/log"
)

type tmpFileTracker struct {
	paths []string
}

func (t *tmpFileTracker) purge(ctx context.Context) error {
	for _, p := range t.paths {
		log.Infof(ctx, "Purging tmp-file: %s", p)
		err := os.Remove(p)
		if err != nil {
			return err
		}
	}
	t.paths = make([]string, 0)
	log.Info(ctx, "All tmp-files purged successfully")
	return nil
}

func (t *tmpFileTracker) mkfile(prefix string) (*os.File, error) {
	file, err := ioutil.TempFile("", prefix)
	if err != nil {
		return nil, err
	}
	t.paths = append(t.paths, file.Name())
	return file, nil
}

func (t *tmpFileTracker) dumpToCsv(name string,
	rows [][]string) (string, error) {
	file, err := t.mkfile(name)
	if err != nil {
		return "", err
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()
	for _, r := range rows {
		writer.Write(r)
	}
	return file.Name(), nil
}
