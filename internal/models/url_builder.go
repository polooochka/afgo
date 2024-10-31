package models

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
)

var (
	EndPoints map[string]string = map[string]string{
		"geo_by_date_report":     "https://hq1.appsflyer.com/api/agg-data/export/app/{{ app-id }}/geo_by_date_report/v5",
		"in-app_events":          "https://hq1.appsflyer.com/api/raw-data/export/app/{{ app-id }}/in_app_events_report/v5",
		"in-app-events-retarget": "https://hq1.appsflyer.com/api/raw-data/export/app/{{ app-id }}/in-app-events-retarget/v5",
	}

	AdditionalFields map[string]string = map[string]string{
		"in-app_events":          "blocked_reason_rule,store_reinstall,impressions,contributor3_match_type,custom_dimension,conversion_type,gp_click_time,match_type,mediation_network,oaid,deeplink_url,blocked_reason,blocked_sub_reason,gp_broadcast_referrer,gp_install_begin,campaign_type,custom_data,rejected_reason,device_download_time,keyword_match_type,contributor1_match_type,contributor2_match_type,device_model,monetization_network,segment,is_lat,gp_referrer,blocked_reason_value,store_product_page,device_category,app_type,rejected_reason_value,ad_unit,keyword_id,placement,network_account_id,install_app_store,amazon_aid,att,engagement_type,gdpr_applies,ad_user_data_enabled,ad_personalization_enabled",
		"in-app-events-retarget": "blocked_reason_rule,store_reinstall,impressions,contributor3_match_type,custom_dimension,conversion_type,gp_click_time,match_type,mediation_network,oaid,deeplink_url,blocked_reason,blocked_sub_reason,gp_broadcast_referrer,gp_install_begin,campaign_type,custom_data,rejected_reason,device_download_time,keyword_match_type,contributor1_match_type,contributor2_match_type,device_model,monetization_network,segment,is_lat,gp_referrer,blocked_reason_value,store_product_page,device_category,app_type,rejected_reason_value,ad_unit,keyword_id,placement,network_account_id,install_app_store,amazon_aid,att,engagement_type,gdpr_applies,ad_user_data_enabled,ad_personalization_enabled",
	}
)

type UrlBuilder struct {
	Path    string            `json:"path"`
	Headers map[string]string `json:"headers"`
	Query   map[string]string `json:"query"`
	Meta    map[string]string `json:"meta"`
	Req     *http.Request     `json:"-"`
	Err     error             `json:"-"`
}

func NewUrlBuilder() UrlBuilder {
	var u UrlBuilder
	u.Headers = make(map[string]string)
	u.Query = make(map[string]string)
	u.Meta = make(map[string]string)
	return u
}

func (u *UrlBuilder) SetPath(path string) {
	u.Path = path
}

func (u *UrlBuilder) SetHeaders(token string) {
	u.Headers = map[string]string{
		"Authorization": "Bearer " + token,
	}
}

func (u *UrlBuilder) AddQueryParams(k string, v string) {
	if v == "" {
		return
	}
	u.Query[k] = v
}

func (u *UrlBuilder) AddMeta(k string, v string) {
	u.Meta[k] = v
}

func (u *UrlBuilder) SetEndpoint(base_endpoint string, app_id string) {
	endpoint := strings.Replace(base_endpoint, "{{ app-id }}", app_id, 1)
	u.SetPath(endpoint)
}

func (u *UrlBuilder) CreateReq() error {
	req, err := http.NewRequest("GET", u.Path, nil)
	if err != nil {
		return err
	}

	for k, v := range u.Headers {
		req.Header.Set(k, v)
	}

	q := req.URL.Query()
	for k, v := range u.Query {
		q.Add(k, v)
	}
	req.URL.RawQuery = q.Encode()

	u.Req = req
	return nil
}

type UrlBuilderRequest interface {
	FromRequest(u *UrlBuilder) error
}

type UrlBuilderAggRequest struct {
	AppId   string `ch:"app_id"`
	Tracker string `ch:"tracker"`
	Cabinet string `ch:"cabinet"`
	Method  string `ch:"method"`
	From    string
	To      string
	Reattr  bool `ch:"reattr"`
}

func (ubr *UrlBuilderAggRequest) FromRequest(u *UrlBuilder) error {
	base_endpoint, ok := EndPoints[ubr.Method]
	if !ok {
		return errors.New("method dont exist in endpoints")
	}
	u.SetEndpoint(base_endpoint, ubr.AppId)

	u.AddQueryParams("from", ubr.From)
	u.AddQueryParams("to", ubr.To)
	u.AddQueryParams("reattr", strconv.FormatBool(ubr.Reattr))

	u.AddMeta("app_id", ubr.AppId)
	u.AddMeta("tracker", ubr.Tracker)
	u.AddMeta("cabinet", ubr.Cabinet)
	u.AddMeta("method", ubr.Method)
	u.AddMeta("from", ubr.From)
	u.AddMeta("to", ubr.To)
	u.AddMeta("reattr", strconv.FormatBool(ubr.Reattr))

	return nil
}

type UrlBuilderRawRequest struct {
	AppId      string `ch:"app_id"`
	Tracker    string `ch:"tracker"`
	Cabinet    string `ch:"cabinet"`
	Method     string `ch:"method"`
	From       string
	To         string
	EventNames []string `ch:"event_names"`
}

func (ubr *UrlBuilderRawRequest) FromRequest(u *UrlBuilder) error {
	base_endpoint, ok := EndPoints[ubr.Method]
	if !ok {
		return errors.New("method dont exist in endpoints")
	}
	u.SetEndpoint(base_endpoint, ubr.AppId)

	u.AddQueryParams("from", ubr.From)
	u.AddQueryParams("to", ubr.To)
	u.AddQueryParams("event_name", strings.Join(ubr.EventNames, ","))
	u.AddQueryParams("maximum_rows", "1000000")

	additionalfields, ok := AdditionalFields[ubr.Method]
	if !ok {
		return errors.New("method dont exist in additionalfields")
	}
	u.AddQueryParams("additional_fields", additionalfields)

	u.AddMeta("app_id", ubr.AppId)
	u.AddMeta("tracker", ubr.Tracker)
	u.AddMeta("cabinet", ubr.Cabinet)
	u.AddMeta("method", ubr.Method)
	u.AddMeta("from", ubr.From)
	u.AddMeta("to", ubr.To)
	u.AddMeta("event_name", strings.Join(ubr.EventNames, ","))

	return nil
}

type UrlBuilderResponse struct {
	Builder *UrlBuilder
	Resp    *http.Response
	Err     error
}
