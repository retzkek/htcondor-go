package htcondor

import "testing"

func TestCondorStatus(t *testing.T) {
	ads, err := NewCommand("condor_status").Run()
	if err != nil {
		t.Error(err)
	}
	t.Log(ads)
}

func TestCondorQ(t *testing.T) {
	ads, err := NewCommand("condor_q").Run()
	if err != nil {
		t.Error(err)
	}
	t.Log(ads)
}

func TestCondorHistory(t *testing.T) {
	ads, err := NewCommand("condor_history").Run()
	if err != nil {
		t.Error(err)
	}
	t.Log(ads)
}
