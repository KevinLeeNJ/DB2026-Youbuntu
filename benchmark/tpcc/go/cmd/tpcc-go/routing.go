package main

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
)

const (
	officialSlotCount         = 160
	officialHotWarehouseCount = 4
	officialHotWarehouseSlots = 26
	officialExtraColdSlots    = 10
	officialHotItemCount      = 24
)

// officialRoutingPlan owns the routing random domain. Hot identities are stable
// for the run, while each phase gets an independently shuffled 160-slot wheel.
type officialRoutingPlan struct {
	hotWarehouses []int
	hotWarehouse  map[int]struct{}
	hotDistrict   map[int]int
	hotItems      []int
	slots         [][]int
	seed          int64
	profile       profile
}

func routingSeed(seed int64, domain, phase int) int64 {
	value := uint64(seed) ^ uint64(domain+1)*0x9e3779b97f4a7c15 ^ uint64(phase+1)*0xbf58476d1ce4e5b9
	value = (value ^ (value >> 30)) * 0xbf58476d1ce4e5b9
	value = (value ^ (value >> 27)) * 0x94d049bb133111eb
	return int64(value ^ (value >> 31))
}

func newOfficialRoutingPlan(seed int64, p profile, phases int) (*officialRoutingPlan, error) {
	if p.warehouses != officialWarehouses {
		return nil, fmt.Errorf("official routing requires %d warehouses, got %d", officialWarehouses, p.warehouses)
	}
	if p.districtsPerWarehouse < 1 || p.itemCount <= officialHotItemCount || phases < 1 {
		return nil, fmt.Errorf("invalid profile for official routing")
	}
	warehouseIDs := make([]int, p.warehouses)
	for i := range warehouseIDs {
		warehouseIDs[i] = i + 1
	}
	identityRNG := rand.New(rand.NewSource(routingSeed(seed, 0, 0)))
	identityRNG.Shuffle(len(warehouseIDs), func(i, j int) { warehouseIDs[i], warehouseIDs[j] = warehouseIDs[j], warehouseIDs[i] })

	plan := &officialRoutingPlan{
		hotWarehouses: append([]int(nil), warehouseIDs[:officialHotWarehouseCount]...),
		hotWarehouse:  make(map[int]struct{}, officialHotWarehouseCount),
		hotDistrict:   make(map[int]int, officialHotWarehouseCount),
		slots:         make([][]int, phases),
		seed:          seed,
		profile:       p,
	}
	sort.Ints(plan.hotWarehouses)
	for _, wID := range plan.hotWarehouses {
		plan.hotWarehouse[wID] = struct{}{}
		plan.hotDistrict[wID] = identityRNG.Intn(p.districtsPerWarehouse) + 1
	}

	itemIDs := make([]int, p.itemCount)
	for i := range itemIDs {
		itemIDs[i] = i + 1
	}
	identityRNG.Shuffle(len(itemIDs), func(i, j int) { itemIDs[i], itemIDs[j] = itemIDs[j], itemIDs[i] })
	plan.hotItems = append([]int(nil), itemIDs[:officialHotItemCount]...)

	cold := make([]int, 0, p.warehouses-officialHotWarehouseCount)
	for wID := 1; wID <= p.warehouses; wID++ {
		if _, hot := plan.hotWarehouse[wID]; !hot {
			cold = append(cold, wID)
		}
	}
	for phase := range plan.slots {
		wheel := make([]int, 0, officialSlotCount)
		for _, wID := range plan.hotWarehouses {
			for i := 0; i < officialHotWarehouseSlots; i++ {
				wheel = append(wheel, wID)
			}
		}
		wheel = append(wheel, cold...)
		extra := append([]int(nil), cold...)
		phaseRNG := rand.New(rand.NewSource(routingSeed(seed, 1, phase)))
		phaseRNG.Shuffle(len(extra), func(i, j int) { extra[i], extra[j] = extra[j], extra[i] })
		wheel = append(wheel, extra[:officialExtraColdSlots]...)
		phaseRNG.Shuffle(len(wheel), func(i, j int) { wheel[i], wheel[j] = wheel[j], wheel[i] })
		if len(wheel) != officialSlotCount {
			return nil, fmt.Errorf("routing wheel has %d slots, want %d", len(wheel), officialSlotCount)
		}
		plan.slots[phase] = wheel
	}
	return plan, nil
}

func officialSlotIndex(clientID int, txnNo uint64) int {
	return (clientID + officialWorkers*int(txnNo%5) + 13*int(txnNo/5)) % officialSlotCount
}

type officialRouter struct {
	plan     *officialRoutingPlan
	clientID int
	phase    int
	txnNo    uint64
	rng      *rand.Rand
}

func newOfficialRouter(plan *officialRoutingPlan, clientID int) *officialRouter {
	return &officialRouter{plan: plan, clientID: clientID, phase: -1}
}

func (r *officialRouter) next(phase int) txnContext {
	if phase < 0 || phase >= len(r.plan.slots) {
		panic("official routing phase out of range")
	}
	if r.phase != phase {
		r.phase = phase
		r.txnNo = 0
		r.rng = rand.New(rand.NewSource(routingSeed(r.plan.seed, 2+r.clientID, phase)))
	}
	wID := r.plan.slots[phase][officialSlotIndex(r.clientID, r.txnNo)]
	r.txnNo++
	dID := r.rng.Intn(r.plan.profile.districtsPerWarehouse) + 1
	if hotDistrict, hot := r.plan.hotDistrict[wID]; hot {
		if r.rng.Intn(100) < 65 || r.plan.profile.districtsPerWarehouse == 1 {
			dID = hotDistrict
		} else {
			dID = r.rng.Intn(r.plan.profile.districtsPerWarehouse-1) + 1
			if dID >= hotDistrict {
				dID++
			}
		}
	}
	return txnContext{
		wID:        wID,
		dID:        dID,
		official:   true,
		hotItemIDs: r.plan.hotItems,
		profile:    r.plan.profile,
	}
}

func applyCoverageGate(result *result, hotWarehouses []int, combined bool) error {
	fullRequirement := 45
	if combined {
		fullRequirement = officialWarehouses
	}
	required := int(math.Ceil(float64(fullRequirement*result.Coverage.Completed) / 400))
	if required > fullRequirement {
		required = fullRequirement
	}
	result.Coverage.RequiredWarehouseCount = required
	result.Coverage.RequireAllHot = result.Coverage.Completed >= 400
	result.Coverage.HotWarehouses = result.Coverage.HotWarehouses[:0]
	for _, wID := range hotWarehouses {
		if _, ok := result.covered[wID]; ok {
			result.Coverage.HotWarehouses = append(result.Coverage.HotWarehouses, wID)
		}
	}
	result.Coverage.HotWarehouseCount = len(result.Coverage.HotWarehouses)
	if result.Coverage.WarehouseCount < required {
		return fmt.Errorf("coverage has %d warehouses from %d completed transactions, want at least %d",
			result.Coverage.WarehouseCount, result.Coverage.Completed, required)
	}
	if result.Coverage.RequireAllHot && result.Coverage.HotWarehouseCount != len(hotWarehouses) {
		return fmt.Errorf("coverage includes %d/%d hot warehouses", result.Coverage.HotWarehouseCount, len(hotWarehouses))
	}
	return nil
}
