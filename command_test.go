package htcondor

import (
	"testing"

	"github.com/retzkek/htcondor-go/classad"
)

func TestCondorStatus(t *testing.T) {
	ads, err := NewCommand("condor_status").Run()
	if err != nil {
		t.Error(err)
	}
	if len(ads) != 2 {
		t.Errorf("expected two ClassAds, got %d", len(ads))
	}
	t.Log(ads)
}

func TestCondorStatusStream(t *testing.T) {
	ads := make(chan classad.ClassAd)
	errors := make(chan error)
	NewCommand("condor_status").Stream(ads, errors)
	n := 0
	for {
		select {
		case ad, ok := <-ads:
			if ok {
				t.Log(ad)
				n++
			} else {
				ads = nil
			}
		case err, ok := <-errors:
			if ok {
				t.Error(err)
			} else {
				errors = nil
			}
		}
		if ads == nil && errors == nil {
			break
		}
	}
	if n != 2 {
		t.Errorf("expected two ClassAds, got %d", n)
	}
}

func TestCondorStatusSchedd(t *testing.T) {
	ads, err := NewCommand("condor_status").WithArg("-schedd").WithAttribute("name").Run()
	if err != nil {
		t.Error(err)
	}
	if len(ads) != 1 {
		t.Errorf("expected one ClassAds, got %d", len(ads))
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
		t.Errorf("expected one ClassAds, got %d", len(ads))
	}
	t.Log(ads)
}
