package classad

import (
	"bufio"
	"fmt"
	"io"
	"strings"
)

// AttributeType represents the supported Classad attribute types
type AttributeType int

// ClassAd attribute types
const (
	Integer AttributeType = iota
	Real
	String
	Undefined
	Error
)

// Attribute represents a typed Classad attribute.
type Attribute struct {
	Type  AttributeType
	Value interface{}
}

// String returns the string representation of the ClassAd attribute.
func (a Attribute) String() string {
	switch a.Type {
	case Integer:
		return fmt.Sprintf("%d", a.Value)
	case Real:
		return fmt.Sprintf("%f", a.Value)
	case String:
		return fmt.Sprintf("%s", a.Value)
	case Undefined:
		return "UNDEFINED"
	case Error:
		return "ERROR"
	}
	return "TYPEERROR"
}

// ClassAd represents an HTCondor ClassAd (see http://research.cs.wisc.edu/htcondor/manual/current/4_1HTCondor_s_ClassAd.html).
type ClassAd map[string]Attribute

// ReadClassAds reads multiple ClassAds (in "long" format) from r until EOF or a blank line.
// ClassAds should be separated by a blank line.
// All attributes are returned as strings (for now).
func ReadClassAds(r io.Reader) ([]ClassAd, error) {
	scanner := bufio.NewScanner(r)
	ads := make([]ClassAd, 0)
	ad := make(ClassAd)
	for scanner.Scan() {
		if scanner.Text() == "" {
			if len(ad) > 0 {
				ads = append(ads, ad)
				ad = make(ClassAd)
			}
			continue
		}
		// Naïve tokenizing and parsing of long format.
		parts := strings.SplitN(scanner.Text(), "=", 2)
		if len(parts) < 2 {
			return nil, fmt.Errorf("invalid classad attribute: \"%s\"", scanner.Text())
		}
		ad[strings.Trim(parts[0], " \"")] = Attribute{
			Type:  String,
			Value: strings.Trim(parts[1], " \""),
		}

	}
	if err := scanner.Err(); err != nil {
		return nil, err
	}
	if len(ad) > 0 {
		ads = append(ads, ad)
	}
	return ads, nil
}

// ReadClassAds reads multiple classads (in "long" format) that are separated by a blank line.
/*func ReadClassAds(r io.Reader) ([]ClassAd, error) {
	ads := make([]ClassAd, 0)
	for {
		ad, err := ReadClassAd(r)
		if err != nil {
			return ads, err
		}
		if ad == nil {
			break
		}
		ads = append(ads, ad)
	}
	return ads, nil
}*/

// Strings returns a map of the string representation for all the attributes in the ClassAd.
func (c ClassAd) Strings() map[string]string {
	ad := make(map[string]string, len(c))
	for k, v := range c {
		ad[k] = v.String()
	}
	return ad
}
