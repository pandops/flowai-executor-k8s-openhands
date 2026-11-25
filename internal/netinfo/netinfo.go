// Package netinfo finds the address of the Executor's primary network
// interface: the interface of its default route. IPv4 of the IPv4 default
// route wins; otherwise the global IPv6 address of the IPv6 default route.
package netinfo

import (
	"bufio"
	"net"
	"os"
	"strings"
)

// Source reports default routes and interface addresses.
type Source interface {
	// DefaultRouteInterface4 returns the interface of the IPv4 default route.
	DefaultRouteInterface4() (string, bool)
	// DefaultRouteInterface6 returns the interface of the IPv6 default route.
	DefaultRouteInterface6() (string, bool)
	// InterfaceAddrs returns the addresses of one interface.
	InterfaceAddrs(name string) ([]net.IPNet, error)
}

// PrimaryIP returns the primary interface address as one canonical literal
// (IPv4 dotted quad or RFC 5952 IPv6), or false when there is none.
func PrimaryIP(src Source) (string, bool) {
	if src == nil {
		return "", false
	}
	if iface, ok := src.DefaultRouteInterface4(); ok {
		addrs, _ := src.InterfaceAddrs(iface)
		for _, a := range addrs {
			if v4 := a.IP.To4(); v4 != nil {
				return v4.String(), true
			}
		}
	}
	if iface, ok := src.DefaultRouteInterface6(); ok {
		addrs, _ := src.InterfaceAddrs(iface)
		for _, a := range addrs {
			if a.IP.To4() == nil && a.IP.IsGlobalUnicast() {
				return a.IP.String(), true
			}
		}
	}
	return "", false
}

// System returns the Source that reads /proc/net/route,
// /proc/net/ipv6_route and the host interfaces.
func System() Source { return system{} }

type system struct{}

func (system) DefaultRouteInterface4() (string, bool) {
	// Columns: Iface Destination Gateway Flags ... Mask
	return scan("/proc/net/route", func(f []string) (string, bool) {
		if len(f) > 7 && f[1] == "00000000" && f[7] == "00000000" {
			return f[0], true
		}
		return "", false
	})
}

func (system) DefaultRouteInterface6() (string, bool) {
	// Columns: dest dest_prefix src src_prefix next_hop metric refcnt use flags iface
	return scan("/proc/net/ipv6_route", func(f []string) (string, bool) {
		if len(f) > 9 && f[0] == strings.Repeat("0", 32) && f[1] == "00" && f[9] != "lo" {
			return f[9], true
		}
		return "", false
	})
}

func (system) InterfaceAddrs(name string) ([]net.IPNet, error) {
	iface, err := net.InterfaceByName(name)
	if err != nil {
		return nil, err
	}
	addrs, err := iface.Addrs()
	if err != nil {
		return nil, err
	}
	var out []net.IPNet
	for _, a := range addrs {
		if n, ok := a.(*net.IPNet); ok {
			out = append(out, *n)
		}
	}
	return out, nil
}

func scan(path string, match func([]string) (string, bool)) (string, bool) {
	f, err := os.Open(path)
	if err != nil {
		return "", false
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	for s.Scan() {
		if name, ok := match(strings.Fields(s.Text())); ok {
			return name, true
		}
	}
	return "", false
}
