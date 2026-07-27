package main

import "testing"

func TestOfficialRoutingPlanIsDeterministicAndPhaseSeparated(t *testing.T) {
	p := profile{warehouses: 50, districtsPerWarehouse: 10, customersPerDistrict: 3000, itemCount: 100000}
	left, err := newOfficialRoutingPlan(91, p, 4)
	if err != nil {
		t.Fatal(err)
	}
	right, err := newOfficialRoutingPlan(91, p, 4)
	if err != nil {
		t.Fatal(err)
	}
	for phase := range left.slots {
		for slot := range left.slots[phase] {
			if left.slots[phase][slot] != right.slots[phase][slot] {
				t.Fatalf("phase %d slot %d is not deterministic", phase, slot)
			}
		}
	}
	same := true
	for slot := range left.slots[0] {
		if left.slots[0][slot] != left.slots[1][slot] {
			same = false
			break
		}
	}
	if same {
		t.Fatal("independent phase wheels are identical")
	}
}

func TestCoverageGateScalesAndRequiresHotWarehousesAt400(t *testing.T) {
	hot := []int{1, 2, 3, 4}
	makeResult := func(completed int, warehouses []int) *result {
		result := newResult(150)
		result.Coverage.Completed = completed
		for _, wID := range warehouses {
			result.covered[wID] = struct{}{}
		}
		result.finalize()
		return result
	}

	covered22 := make([]int, 22)
	for i := range covered22 {
		covered22[i] = i + 1
	}
	if err := applyCoverageGate(makeResult(200, covered22), hot, false); err == nil {
		t.Fatal("200 completions with 22 warehouses passed the ceil(45*200/400)=23 gate")
	}
	covered23 := append(covered22, 23)
	if err := applyCoverageGate(makeResult(200, covered23), hot, false); err != nil {
		t.Fatal(err)
	}

	covered45 := make([]int, 45)
	for i := range covered45 {
		covered45[i] = i + 1
	}
	if err := applyCoverageGate(makeResult(400, covered45), []int{1, 2, 3, 50}, false); err == nil {
		t.Fatal("400 completions without every hot warehouse passed")
	}
	if err := applyCoverageGate(makeResult(400, covered45), hot, false); err != nil {
		t.Fatal(err)
	}
}
