package htcondor

import (
	"testing"

	"github.com/golang/groupcache"
	"github.com/retzkek/htcondor-go/classad"
)

var (
	cache *groupcache.HTTPPool
)

func init() {
	cache = groupcache.NewHTTPPool("http://localhost:8080")
}

func stream(cmd *Command) ([]classad.ClassAd, error) {
	adch := make(chan classad.ClassAd)
	ads := make([]classad.ClassAd, 0)
	errors := make(chan error)
	go cmd.Stream(adch, errors)
	n := 0
	for {
		select {
		case ad, ok := <-adch:
			if ok {
				ads = append(ads, ad)
				n++
			} else {
				adch = nil
			}
		case err, ok := <-errors:
			if ok {
				return ads, err
			} else {
				errors = nil
			}
		}
		if adch == nil && errors == nil {
			break
		}
	}
	return ads, nil

}

func TestCondorStatus(t *testing.T) {
	ads, err := NewCommand("condor_status").Run()
	if err != nil {
		t.Error(err)
	}
	if len(ads) != 1 {
		t.Errorf("condor_status expected one ClassAd, got %d", len(ads))
	}
	t.Log(ads)
}

func TestCondorStatusStream(t *testing.T) {
	cmd := NewCommand("condor_status")
	ads, err := stream(cmd)
	if err != nil {
		t.Error(err)
	}
	if len(ads) != 1 {
		t.Errorf("condor_status expected one ClassAd, got %d", len(ads))
	}
	t.Log(ads)
}

func TestCondorStatusCache(t *testing.T) {
	cmd := NewCommand("condor_status").WithCache(cache, "status", 64<<20, 0)
	ads, err := cmd.Run()
	if err != nil {
		t.Error(err)
	}
	if len(ads) != 1 {
		t.Errorf("condor_status expected one ClassAd, got %d", len(ads))
	}
	// second time, should hit cache
	ads, err = cmd.Run()
	if err != nil {
		t.Error(err)
	}
	if len(ads) != 1 {
		t.Errorf("condor_status expected one ClassAd, got %d", len(ads))
	}
	stats := groupcache.GetGroup("status").CacheStats(groupcache.MainCache)
	if stats.Hits != 1 {
		t.Errorf("expected one cache hit, got %d", stats.Hits)
	}
	t.Log(ads)
}

func TestCondorStatusCacheStream(t *testing.T) {
	cmd := NewCommand("condor_status").WithCache(cache, "status_stream", 64<<20, 0)
	ads, err := stream(cmd)
	if err != nil {
		t.Error(err)
	}
	if len(ads) != 1 {
		t.Errorf("condor_status expected one ClassAd, got %d", len(ads))
	}
	// second time, should hit cache
	ads, err = stream(cmd)
	if err != nil {
		t.Error(err)
	}
	stats := groupcache.GetGroup("status").CacheStats(groupcache.MainCache)
	if stats.Hits != 1 {
		t.Errorf("expected one cache hit, got %d", stats.Hits)
	}
	t.Log(ads)
}

func TestCondorStatusSchedd(t *testing.T) {
	ads, err := NewCommand("condor_status").WithArg("-schedd").WithAttribute("name").Run()
	if err != nil {
		t.Error(err)
	}
	if len(ads) != 1 {
		t.Errorf("expected one ClassAd, got %d", len(ads))
	}
	t.Log(ads)
}

func TestCondorStatusScheddStream(t *testing.T) {
	cmd := NewCommand("condor_status").WithArg("-schedd").WithAttribute("name")
	ads, err := stream(cmd)
	if err != nil {
		t.Error(err)
	}
	if len(ads) != 1 {
		t.Errorf("expected one ClassAd, got %d", len(ads))
	}
	t.Log(ads)
}

func TestCondorQ(t *testing.T) {
	ads, err := NewCommand("condor_q").Run()
	if err != nil {
		t.Error(err)
	}
	if len(ads) != 1 {
		t.Errorf("expected one ClassAd, got %d", len(ads))
	}
	t.Log(ads)
}

func TestCondorQStream(t *testing.T) {
	cmd := NewCommand("condor_q")
	ads, err := stream(cmd)
	if err != nil {
		t.Error(err)
	}
	if len(ads) != 1 {
		t.Errorf("expected one ClassAd, got %d", len(ads))
	}
	t.Log(ads)
}

func TestCondorHistory(t *testing.T) {
	ads, err := NewCommand("condor_history").Run()
	if err != nil {
		t.Error(err)
	}
	if len(ads) != 1 {
		t.Errorf("expected one ClassAd, got %d", len(ads))
	}
	t.Log(ads)
}
func TestCondorHistoryStream(t *testing.T) {
	cmd := NewCommand("condor_history")
	ads, err := stream(cmd)
	if err != nil {
		t.Error(err)
	}
	if len(ads) != 1 {
		t.Errorf("expected one ClassAd, got %d", len(ads))
	}
	t.Log(ads)
}

func TestCondorHistoryConstraint(t *testing.T) {
	ads, err := NewCommand("condor_history").WithConstraint("false").Run()
	if err != nil {
		t.Error(err)
	}
	if len(ads) != 0 {
		t.Errorf("expected zero ClassAds, got %d", len(ads))
	}
	t.Log(ads)
}

func TestCondorHistoryAttribute(t *testing.T) {
	ads, err := NewCommand("condor_history").WithAttribute("LastRemoteHost").Run()
	if err != nil {
		t.Error(err)
	}
	if len(ads) != 1 {
		t.Errorf("expected one ClassAd, got %d", len(ads))
	}
	t.Log(ads)
}
