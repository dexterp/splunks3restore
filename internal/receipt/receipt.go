package receipt

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"strings"
	"sync"
)

var chash1 = regexp.MustCompile(`"content_hash":\s*"([^"]+)"\s*,`)
var chash2 = regexp.MustCompile(`,\s*"content_hash":\s*"([^"]+)"`)
var nochash1 = regexp.MustCompile(`("cipher_blob":"[^"]+")(,?)`)

type ReceiptJson struct {
	Path           string
	ContentHash    string
	CalculatedHash string
	matched        bool
	mu             *sync.Mutex
}

func New(fpath string) *ReceiptJson {
	r := &ReceiptJson{mu: &sync.Mutex{}}
	sethash(r, fpath)
	return r
}

// HashesMatch returns true if the receipt.json content_hash and the calculated hash matches
func (r *ReceiptJson) HashesMatch() bool {
	return r.matched
}

// ZeroFrozenInCluster sets the value frozen_in_cluster to 0. Returns path to receipt file.
//
// If update is true it will replace the existing file.
func (r *ReceiptJson) ZeroFrozenInCluster(update bool) (string, error) {
	var newpath string
	r.mu.Lock()
	regex1 := regexp.MustCompile(`"frozen_in_cluster":(\s*)"1"`)
	f, err := ioutil.TempFile("/tmp", "resetfrozen-*-receipt.json")
	if err != nil {
		return "", err
	}
	newpath = f.Name()
	scanner := func(buf []byte) {
		matches := regex1.FindSubmatch(buf)
		if len(matches) == 2 {
			newsetting := fmt.Sprintf(`"frozen_in_cluster":%s"0"`, matches[1])
			buf = regex1.ReplaceAll(buf, []byte(newsetting))
		}

		_, err := f.Write(buf)
		if err != nil {
			log.Printf("restore sta")
		}
	}
	scan(r.Path, scanner)
	f.Close()
	r.mu.Unlock()
	New(newpath).ResetContentHash(true)
	if update {
		err = r.replaceSelf(newpath)
		if err != nil {
			return "", err
		}
		return r.Path, nil
	}
	return newpath, nil
}

// ResetContentHash updates the content_hash field. Returns path to receipt file which can be moved.
func (r *ReceiptJson) ResetContentHash(update bool) (string, error) {
	var newpath string
	r.mu.Lock()
	f, err := ioutil.TempFile("/tmp", "fixup-*-receipt.json")
	if err != nil {
		return "", err
	}
	newpath = f.Name()
	rplc1 := []byte(fmt.Sprintf(`"content_hash":"%s",`, r.CalculatedHash))
	rplc2 := []byte(fmt.Sprintf(`,"content_hash":"%s"`, r.CalculatedHash))
	replaced := false
	scanner := func(buf []byte) {
		if replaced {
			f.Write(buf)
			return
		}
		if r.ContentHash != "" {
			match1 := chash1.FindSubmatch(buf)
			if len(match1) >= 2 {
				buf = chash1.ReplaceAll(buf, rplc1)
				replaced = true
			}

			match2 := chash2.FindSubmatch(buf)
			if len(match2) >= 2 {
				buf = chash2.ReplaceAll(buf, rplc2)
				replaced = true
			}
		} else {
			match3 := nochash1.FindSubmatch(buf)
			if r.ContentHash == "" && len(match3) >= 3 {
				newhash := append(match3[1], rplc2...)
				newhash = append(newhash, match3[2]...)
				buf = nochash1.ReplaceAll(buf, newhash)
				replaced = true
			}
		}
		f.Write(buf)
	}
	scan(r.Path, scanner)
	f.Close()
	r.mu.Unlock()
	if update {
		err = r.replaceSelf(newpath)
		if err != nil {
			return "", err
		}
		return r.Path, nil
	}
	return newpath, nil
}

// replaceSelf Replace the current Path with contents of fpath and recalculates the hashes
func (r *ReceiptJson) replaceSelf(fpath string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	n := New(fpath)
	r.ContentHash = n.ContentHash
	r.CalculatedHash = n.CalculatedHash
	r.matched = n.matched
	return os.Rename(fpath, r.Path)
}

func scan(fpath string, fn func([]byte)) {
	reader, err := os.Open(fpath)
	defer reader.Close()
	if err != nil {
		panic(err)
	}
	scanner := bufio.NewScanner(reader)
	err = scanner.Err()
	if err != nil {
		panic(err)
	}

	for scanner.Scan() {
		buf := scanner.Bytes()
		fn(buf)
	}
}

// setHash scans fpath as a "receipt.json" and updates receipt.
func sethash(receipt *ReceiptJson, fpath string) {
	contentH, calcHash, matched := checkHash(fpath)
	receipt.Path = fpath
	receipt.ContentHash = contentH
	receipt.CalculatedHash = calcHash
	receipt.matched = matched
}

// checkHash checks receipt.json content_hash for a match
// contentHash is the hash contained in the receipt.json
// calculatedHash is the hash contained in the receipt.json
// matched is true if the hash sum matches
func checkHash(fpath string) (contentHash string, calculatedHash string, matched bool) {
	var curSha256hex []byte
	hash := sha256.New()
	scanner := func(buf []byte) {
		match1 := chash1.FindSubmatch(buf)
		if len(match1) >= 2 {
			curSha256hex = match1[1]
			buf = chash1.ReplaceAll(buf, []byte{})
		}

		if len(curSha256hex) == 0 {
			match2 := chash2.FindSubmatch(buf)
			if len(match2) >= 2 {
				curSha256hex = match2[1]
				buf = chash2.ReplaceAll(buf, []byte{})
			}
		}

		_, err := hash.Write(buf)
		if err != nil {
			log.Printf("restore component=checkhash status=failure")
		}
	}
	scan(fpath, scanner)

	sum := hash.Sum(nil)
	hex := strings.ToUpper(fmt.Sprintf("%x", sum))
	calcSha256Hex := []byte(hex)

	match := bytes.Compare(curSha256hex, calcSha256Hex) == 0
	return string(curSha256hex), string(calcSha256Hex), match
}
