// Copyright 2018-20 PJ Engineering and Business Solutions Pty. Ltd. All rights reserved.

package faker

import (
	"fmt"
	"golang.org/x/exp/rand"
	"reflect"
	"strings"
	"time"

	"github.com/brianvoe/gofakeit/v4"

	dataframe "github.com/rocketlaunchr/dataframe-go"
)

type s struct {
	Name     string
	Function string
	Args     []interface{}
	ProbNil  float64
}

// S is used to create a new Series with "fake" data.
// probNil is the probability of a row being nil.
// fn is a selected function from https://godoc.org/github.com/brianvoe/gofakeit and
// args are the arguments to fn.
func S(name string, probNil float64, fn string, args ...interface{}) s {
	return s{
		Name:     name,
		ProbNil:  probNil,
		Function: fn,
		Args:     args,
	}
}

// NewDataFrame will create a new DataFrame with fake data.
//
// Example:
//
//  import "golang.org/x/exp/rand"
//  import "time"
//
//  src := rand.NewSource(uint64(time.Now().UTC().UnixNano()))
//  df := faker.NewDataFrame(5, src, faker.S("name", 0, "FirstName"), faker.S("email", 0, "Email"))
//
func NewDataFrame(rows int, src rand.Source, s ...s) *dataframe.DataFrame {

	if rows <= 0 {
		panic("rows must be greater that 0")
	}

	var rng *rand.Rand
	if src != nil {
		rng = rand.New(src)
	}

	seriess := []dataframe.Series{}
	for _, val := range s {

		name := val.Name
		funcName := val.Function
		probNil := val.ProbNil

		f, found := FuncMap[strings.ToLower(funcName)]
		if !found {
			panic(fmt.Errorf("%s: function not found", funcName))
		}

		// Call Args
		callArgs := []reflect.Value{}
		for _, v := range val.Args {
			callArgs = append(callArgs, reflect.ValueOf(v))
		}

		rf := reflect.ValueOf(f)

		// Determine return type
		sTyp := rf.Call(callArgs)[0].Interface()

		// Generate values
		gval := []interface{}{}
		for idx := 0; idx < rows; idx++ {
			if probNil == 0 {
				retVals := rf.Call(callArgs)
				retVal := retVals[0].Interface()

				gval = append(gval, retVal)
			} else {
				if rng.Float64() < probNil {
					gval = append(gval, nil)
				} else {
					retVals := rf.Call(callArgs)
					retVal := retVals[0].Interface()

					gval = append(gval, retVal)
				}
			}
		}

		switch sTyp.(type) {
		case string:
			seriess = append(seriess, dataframe.NewSeriesString(name, nil, gval...))
		case time.Time:
			seriess = append(seriess, dataframe.NewSeriesTime(name, nil, gval...))
		case int, int8, int16, int32, int64, bool:
			seriess = append(seriess, dataframe.NewSeriesInt64(name, nil, gval...))
		case float64:
			seriess = append(seriess, dataframe.NewSeriesFloat64(name, nil, gval...))
		}
	}

	return dataframe.NewDataFrame(seriess...)
}

// FuncMap contains functions from https://godoc.org/github.com/brianvoe/gofakeit .
var FuncMap = map[string]interface{}{
	strings.ToLower("Animal"): func() string {
		return gofakeit.Animal()
	},

	strings.ToLower("AnimalType"): func() string {
		return gofakeit.AnimalType()
	},

	strings.ToLower("BS"): func() string {
		return gofakeit.BS()
	},

	strings.ToLower("BeerAlcohol"): func() string {
		return gofakeit.BeerAlcohol()
	},

	strings.ToLower("BeerBlg"): func() string {
		return gofakeit.BeerBlg()
	},

	strings.ToLower("BeerHop"): func() string {
		return gofakeit.BeerHop()
	},

	strings.ToLower("BeerIbu"): func() string {
		return gofakeit.BeerIbu()
	},

	strings.ToLower("BeerMalt"): func() string {
		return gofakeit.BeerMalt()
	},

	strings.ToLower("BeerName"): func() string {
		return gofakeit.BeerName()
	},

	strings.ToLower("BeerStyle"): func() string {
		return gofakeit.BeerStyle()
	},

	strings.ToLower("BeerYeast"): func() string {
		return gofakeit.BeerYeast()
	},

	strings.ToLower("Bool"): func() bool {
		return gofakeit.Bool()
	},

	strings.ToLower("BuzzWord"): func() string {
		return gofakeit.BuzzWord()
	},

	strings.ToLower("CarMaker"): func() string {
		return gofakeit.CarMaker()
	},

	strings.ToLower("CarModel"): func() string {
		return gofakeit.CarModel()
	},

	strings.ToLower("Cat"): func() string {
		return gofakeit.Cat()
	},

	strings.ToLower("ChromeUserAgent"): func() string {
		return gofakeit.ChromeUserAgent()
	},

	strings.ToLower("City"): func() string {
		return gofakeit.City()
	},

	strings.ToLower("Color"): func() string {
		return gofakeit.Color()
	},

	strings.ToLower("Company"): func() string {
		return gofakeit.Company()
	},

	strings.ToLower("CompanySuffix"): func() string {
		return gofakeit.CompanySuffix()
	},

	strings.ToLower("Country"): func() string {
		return gofakeit.Country()
	},

	strings.ToLower("CountryAbr"): func() string {
		return gofakeit.CountryAbr()
	},

	strings.ToLower("CreditCardCvv"): func() string {
		return gofakeit.CreditCardCvv()
	},

	strings.ToLower("CreditCardExp"): func() string {
		return gofakeit.CreditCardExp()
	},

	strings.ToLower("CreditCardNumber"): func() int {
		return gofakeit.CreditCardNumber()
	},

	strings.ToLower("CreditCardNumberLuhn"): func() int {
		return gofakeit.CreditCardNumberLuhn()
	},

	strings.ToLower("CreditCardType"): func() string {
		return gofakeit.CreditCardType()
	},

	strings.ToLower("CurrencyLong"): func() string {
		return gofakeit.CurrencyLong()
	},

	strings.ToLower("CurrencyShort"): func() string {
		return gofakeit.CurrencyShort()
	},

	strings.ToLower("Date"): func() time.Time {
		return gofakeit.Date()
	},

	strings.ToLower("DateRange"): func(start, end time.Time) time.Time {
		return gofakeit.DateRange(start, end)
	},

	strings.ToLower("Day"): func() int {
		return gofakeit.Day()
	},

	strings.ToLower("Digit"): func() string {
		return gofakeit.Digit()
	},

	strings.ToLower("Dog"): func() string {
		return gofakeit.Dog()
	},

	strings.ToLower("DomainName"): func() string {
		return gofakeit.DomainName()
	},

	strings.ToLower("DomainSuffix"): func() string {
		return gofakeit.DomainSuffix()
	},

	strings.ToLower("Email"): func() string {
		return gofakeit.Email()
	},

	strings.ToLower("Extension"): func() string {
		return gofakeit.Extension()
	},

	strings.ToLower("FarmAnimal"): func() string {
		return gofakeit.FarmAnimal()
	},

	strings.ToLower("FirefoxUserAgent"): func() string {
		return gofakeit.FirefoxUserAgent()
	},

	strings.ToLower("FirstName"): func() string {
		return gofakeit.FirstName()
	},

	strings.ToLower("Float64"): func() float64 {
		return gofakeit.Float64()
	},

	strings.ToLower("Float64Range"): func(min, max float64) float64 {
		return gofakeit.Float64Range(min, max)
	},

	strings.ToLower("FuelType"): func() string {
		return gofakeit.FuelType()
	},

	strings.ToLower("Gender"): func() string {
		return gofakeit.Gender()
	},

	strings.ToLower("Generate"): func(dataVal string) string {
		return gofakeit.Generate(dataVal)
	},

	strings.ToLower("HTTPMethod"): func() string {
		return gofakeit.HTTPMethod()
	},

	strings.ToLower("HackerAbbreviation"): func() string {
		return gofakeit.HackerAbbreviation()
	},

	strings.ToLower("HackerAdjective"): func() string {
		return gofakeit.HackerAdjective()
	},

	strings.ToLower("HackerIngverb"): func() string {
		return gofakeit.HackerIngverb()
	},

	strings.ToLower("HackerNoun"): func() string {
		return gofakeit.HackerNoun()
	},

	strings.ToLower("HackerPhrase"): func() string {
		return gofakeit.HackerPhrase()
	},

	strings.ToLower("HackerVerb"): func() string {
		return gofakeit.HackerVerb()
	},

	strings.ToLower("HexColor"): func() string {
		return gofakeit.HexColor()
	},

	strings.ToLower("HipsterParagraph"): func(paragraphCount int, sentenceCount int, wordCount int, separator string) string {
		return gofakeit.HipsterParagraph(paragraphCount, sentenceCount, wordCount, separator)
	},

	strings.ToLower("HipsterSentence"): func(wordCount int) string {
		return gofakeit.HipsterSentence(wordCount)
	},

	strings.ToLower("HipsterWord"): func() string {
		return gofakeit.HipsterWord()
	},

	strings.ToLower("Hour"): func() int {
		return gofakeit.Hour()
	},

	strings.ToLower("IPv4Address"): func() string {
		return gofakeit.IPv4Address()
	},

	strings.ToLower("IPv6Address"): func() string {
		return gofakeit.IPv6Address()
	},

	strings.ToLower("ImageURL"): func(width int, height int) string {
		return gofakeit.ImageURL(width, height)
	},

	strings.ToLower("JobDescriptor"): func() string {
		return gofakeit.JobDescriptor()
	},

	strings.ToLower("JobLevel"): func() string {
		return gofakeit.JobLevel()
	},

	strings.ToLower("JobTitle"): func() string {
		return gofakeit.JobTitle()
	},

	strings.ToLower("Language"): func() string {
		return gofakeit.Language()
	},

	strings.ToLower("LanguageAbbreviation"): func() string {
		return gofakeit.LanguageAbbreviation()
	},

	strings.ToLower("LastName"): func() string {
		return gofakeit.LastName()
	},

	strings.ToLower("Latitude"): func() float64 {
		return gofakeit.Latitude()
	},

	strings.ToLower("LatitudeInRange"): func(min, max float64) float64 {
		x, err := gofakeit.LatitudeInRange(min, max)
		if err != nil {
			panic(err)
		}
		return x
	},

	strings.ToLower("Letter"): func() string {
		return gofakeit.Letter()
	},

	strings.ToLower("Lexify"): func(str string) string {
		return gofakeit.Lexify(str)
	},

	strings.ToLower("LogLevel"): func(logType string) string {
		return gofakeit.LogLevel(logType)
	},

	strings.ToLower("Longitude"): func() float64 {
		return gofakeit.Longitude()
	},

	strings.ToLower("LongitudeInRange"): func(min, max float64) float64 {
		x, err := gofakeit.LongitudeInRange(min, max)
		if err != nil {
			panic(err)
		}
		return x
	},

	strings.ToLower("MacAddress"): func() string {
		return gofakeit.MacAddress()
	},

	strings.ToLower("MimeType"): func() string {
		return gofakeit.MimeType()
	},

	strings.ToLower("Minute"): func() int {
		return gofakeit.Minute()
	},

	strings.ToLower("Month"): func() string {
		return gofakeit.Month()
	},

	strings.ToLower("Name"): func() string {
		return gofakeit.Name()
	},

	strings.ToLower("NamePrefix"): func() string {
		return gofakeit.NamePrefix()
	},

	strings.ToLower("NameSuffix"): func() string {
		return gofakeit.NameSuffix()
	},

	strings.ToLower("NanoSecond"): func() int {
		return gofakeit.NanoSecond()
	},

	strings.ToLower("Number"): func(min int, max int) int {
		return gofakeit.Number(min, max)
	},

	strings.ToLower("Numerify"): func(str string) string {
		return gofakeit.Numerify(str)
	},

	strings.ToLower("OperaUserAgent"): func() string {
		return gofakeit.OperaUserAgent()
	},

	strings.ToLower("Paragraph"): func(paragraphCount int, sentenceCount int, wordCount int, separator string) string {
		return gofakeit.Paragraph(paragraphCount, sentenceCount, wordCount, separator)
	},

	strings.ToLower("Password"): func(lower bool, upper bool, numeric bool, special bool, space bool, num int) string {
		return gofakeit.Password(lower, upper, numeric, special, space, num)
	},

	strings.ToLower("PetName"): func() string {
		return gofakeit.PetName()
	},

	strings.ToLower("Phone"): func() string {
		return gofakeit.Phone()
	},

	strings.ToLower("PhoneFormatted"): func() string {
		return gofakeit.PhoneFormatted()
	},

	strings.ToLower("Price"): func(min, max float64) float64 {
		return gofakeit.Price(min, max)
	},

	strings.ToLower("ProgrammingLanguage"): func() string {
		return gofakeit.ProgrammingLanguage()
	},

	strings.ToLower("ProgrammingLanguageBest"): func() string {
		return gofakeit.ProgrammingLanguageBest()
	},

	strings.ToLower("Question"): func() string {
		return gofakeit.Question()
	},

	strings.ToLower("Quote"): func() string {
		return gofakeit.Quote()
	},

	strings.ToLower("RandString"): func(a []string) string {
		return gofakeit.RandString(a)
	},

	strings.ToLower("SSN"): func() string {
		return gofakeit.SSN()
	},

	strings.ToLower("SafariUserAgent"): func() string {
		return gofakeit.SafariUserAgent()
	},

	strings.ToLower("SafeColor"): func() string {
		return gofakeit.SafeColor()
	},

	strings.ToLower("Second"): func() int {
		return gofakeit.Second()
	},

	strings.ToLower("Sentence"): func(wordCount int) string {
		return gofakeit.Sentence(wordCount)
	},

	strings.ToLower("SimpleStatusCode"): func() int {
		return gofakeit.SimpleStatusCode()
	},

	strings.ToLower("State"): func() string {
		return gofakeit.State()
	},

	strings.ToLower("StateAbr"): func() string {
		return gofakeit.StateAbr()
	},

	strings.ToLower("StatusCode"): func() int {
		return gofakeit.StatusCode()
	},

	strings.ToLower("Street"): func() string {
		return gofakeit.Street()
	},

	strings.ToLower("StreetName"): func() string {
		return gofakeit.StreetName()
	},

	strings.ToLower("StreetNumber"): func() string {
		return gofakeit.StreetNumber()
	},

	strings.ToLower("StreetPrefix"): func() string {
		return gofakeit.StreetPrefix()
	},

	strings.ToLower("StreetSuffix"): func() string {
		return gofakeit.StreetSuffix()
	},

	strings.ToLower("TimeZone"): func() string {
		return gofakeit.TimeZone()
	},

	strings.ToLower("TimeZoneAbv"): func() string {
		return gofakeit.TimeZoneAbv()
	},

	strings.ToLower("TimeZoneFull"): func() string {
		return gofakeit.TimeZoneFull()
	},

	strings.ToLower("TimeZoneOffset"): func() float64 {
		return float64(gofakeit.TimeZoneOffset())
	},

	strings.ToLower("TransmissionGearType"): func() string {
		return gofakeit.TransmissionGearType()
	},

	strings.ToLower("URL"): func() string {
		return gofakeit.URL()
	},

	strings.ToLower("UUID"): func() string {
		return gofakeit.UUID()
	},

	strings.ToLower("UserAgent"): func() string {
		return gofakeit.UserAgent()
	},

	strings.ToLower("Username"): func() string {
		return gofakeit.Username()
	},

	strings.ToLower("VehicleType"): func() string {
		return gofakeit.VehicleType()
	},

	strings.ToLower("WeekDay"): func() string {
		return gofakeit.WeekDay()
	},

	strings.ToLower("Word"): func() string {
		return gofakeit.Word()
	},

	strings.ToLower("Year"): func() int {
		return gofakeit.Year()
	},

	strings.ToLower("Zip"): func() string {
		return gofakeit.Zip()
	},
}
