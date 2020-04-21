package internal

import (
	"crypto/sha1"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

// bid2path converts a bid to an s3 smart store path
func bid2path(bid string) (path string, err error) {
	re, err := regexp.Compile(`^([^~]+)~(\S*)`)
	if err != nil {
		panic(err)
	}
	mtchs := re.FindAllStringSubmatch(bid, -1)
	if len(mtchs[0]) != 3 {
		return "", errors.New("bid is not recognized as a valid pid")
	}
	idx := mtchs[0][1]
	bkt := mtchs[0][2]
	hsh := sha1.New()
	hsh.Write([]byte(bkt))
	bytehsh := hsh.Sum(nil)
	strhsh := fmt.Sprintf("%x", bytehsh)

	dre, err := regexp.Compile(`^(\S{2})(\S{2})`)
	if err != nil {
		panic(err)
	}

	dmtchs := dre.FindAllStringSubmatch(strhsh, -1)
	pth := strings.Join([]string{idx, "db", strings.ToUpper(dmtchs[0][1]), strings.ToUpper(dmtchs[0][2]), bkt}, "/")
	return pth, nil
}
