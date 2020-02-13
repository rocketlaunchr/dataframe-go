// Copyright 2019-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package utime

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
)

var re = regexp.MustCompile(`^(\d+Y)?(\d+M)?(\d+W)?(\d+D)?$`)

type parsed struct {
	years  int
	months int
	weeks  int
	days   int
}

func (p parsed) isZero() bool {
	if p.years == 0 && p.months == 0 && p.weeks == 0 && p.days == 0 {
		return true
	}
	return false
}

func (p parsed) String() string {

	if p.isZero() {
		return ""
	}

	var out string

	// Convert days to weeks
	rem := p.days % 7
	p.weeks = p.weeks + p.days/7
	p.days = rem

	if p.years != 0 {
		out = fmt.Sprintf("%dY", p.years)
	}

	if p.months != 0 {
		out = out + fmt.Sprintf("%dM", p.months)
	}

	if p.weeks != 0 {
		out = out + fmt.Sprintf("%dW", p.weeks)
	}

	if p.days != 0 {
		out = out + fmt.Sprintf("%dD", p.days)
	}

	return out
}

func (p parsed) addDate(reverse bool) (int, int, int) {
	if reverse {
		return -p.years, -p.months, -7*p.weeks - p.days
	}
	return p.years, p.months, 7*p.weeks + p.days
}

func parse(s string) (parsed, error) {
	matches := re.FindStringSubmatch(s)
	if len(matches) == 0 {
		return parsed{}, errors.New("could not parse")
	}
	return parsed{
		years:  parseComponent(matches[1]),
		months: parseComponent(matches[2]),
		weeks:  parseComponent(matches[3]),
		days:   parseComponent(matches[4]),
	}, nil
}

func parseComponent(s string) int {
	if s == "" {
		return 0
	}
	s = s[0 : len(s)-1] // Remove last letter
	n, _ := strconv.Atoi(s)
	return n
}
