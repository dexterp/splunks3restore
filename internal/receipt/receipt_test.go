package receipt

import (
	"path/filepath"
	"runtime"
	"testing"
)

func getPath(path string) string {
	_, filename, _, _ := runtime.Caller(1)
	d := filepath.Dir(filename)
	fp := filepath.Join(d, path)
	fp, _ = filepath.Abs(fp)
	return fp
}

func TestReceiptJson_Matched(t *testing.T) {
	fp := getPath("../../test/fixtures/testdata/receipt.json-ok")

	r := New(fp)
	if !r.HashesMatch() {
		t.Errorf("File: %s content \"%s\" & calculated \"%s\" hashes do not match", fp, r.ContentHash, r.CalculatedHash)
	}
}

func TestReceiptJson_Matched2(t *testing.T) {
	for _, p := range []string{"receipt.json-invalidhash", "receipt.json-nohash"} {
		fp := getPath(filepath.Join("../../test/fixtures/testdata/", p))
		r := New(fp)
		if r.HashesMatch() {
			t.Errorf("Path %s content_hash and calculated hash was expected not to match", r.Path)
		}
	}
}

func TestReceiptJson_ResetContentHash(t *testing.T) {
	for _, p := range []string{"receipt.json-invalidhash", "receipt.json-nohash"} {
		fp := getPath(filepath.Join("../../test/fixtures/testdata/", p))
		orig := New(fp)
		if orig.HashesMatch() {
			t.Errorf("Path %s content_hash and calculated hash was expected not to match", orig.Path)
		}

		fpnew, err := orig.ResetContentHash(false)
		if err != nil {
			t.Errorf("Error resetting hash: %v", err)
		}
		n := New(fpnew)

		if !n.HashesMatch() {
			t.Errorf("Path %s content_hash and calculated hash was expected match. Original file %s", n.Path, orig.Path)
		}
	}
}

func TestReceiptJson_ZeroFrozenInCluster(t *testing.T) {
	for _, p := range []string{"receipt.json-ok", "receipt.json-invalidhash", "receipt.json-nohash"} {
		fp := getPath(filepath.Join("../../test/fixtures/testdata/", p))
		orig := New(fp)

		fpnew, err := orig.ZeroFrozenInCluster(false)
		if err != nil {
			t.Errorf("Error zeroing frozen_in_cluster: %v", err)
		}
		new := New(fpnew)

		if !new.HashesMatch() {
			t.Errorf("Zeroed out file does not have matching content & calculated hashes: Original file: %s, Zeroed file: %s", fp, fpnew)
		}

		if orig.CalculatedHash == new.CalculatedHash {
			t.Errorf("Expected original and zeroed hashes to be different. original: %s \"%s\", zeroed: %s \"%s\"", fp, orig.CalculatedHash, fpnew, new.CalculatedHash)
		}

	}
}
