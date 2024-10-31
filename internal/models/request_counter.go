package models

type RequestCounter struct {
	Path    string `ch:"path"`
	Tracker string `ch:"tracker"`
	Cabinet string `ch:"cabinet"`
	Day     string `ch:"day"`
}
