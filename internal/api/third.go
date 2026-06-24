// Copyright © 2023 OpenIM. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package api

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"fmt"
	"io"
	"math/rand"
	"mime/multipart"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/openimsdk/protocol/constant"
	"github.com/openimsdk/protocol/third"
	"github.com/openimsdk/tools/a2r"
	"github.com/openimsdk/tools/errs"
	"github.com/openimsdk/tools/mcontext"
	"google.golang.org/grpc"
)

type ThirdApi struct {
	GrafanaUrl string
	Client     third.ThirdClient
}

func NewThirdApi(client third.ThirdClient, grafanaUrl string) ThirdApi {
	return ThirdApi{Client: client, GrafanaUrl: grafanaUrl}
}

func (o *ThirdApi) FcmUpdateToken(c *gin.Context) {
	a2r.Call(c, third.ThirdClient.FcmUpdateToken, o.Client)
}

func (o *ThirdApi) SetAppBadge(c *gin.Context) {
	a2r.Call(c, third.ThirdClient.SetAppBadge, o.Client)
}

// #################### s3 ####################

func setURLPrefixOption[A, B, C any](_ func(client C, ctx context.Context, req *A, options ...grpc.CallOption) (*B, error), fn func(*A) error) *a2r.Option[A, B] {
	return &a2r.Option[A, B]{
		BindAfter: fn,
	}
}

func setURLPrefix(c *gin.Context, urlPrefix *string) error {
	host := c.GetHeader("X-Request-Api")
	if host != "" {
		if strings.HasSuffix(host, "/") {
			*urlPrefix = host + "object/"
			return nil
		} else {
			*urlPrefix = host + "/object/"
			return nil
		}
	}
	u := url.URL{
		Scheme: "http",
		Host:   c.Request.Host,
		Path:   "/object/",
	}
	if c.Request.TLS != nil {
		u.Scheme = "https"
	}
	*urlPrefix = u.String()
	return nil
}

func (o *ThirdApi) PartLimit(c *gin.Context) {
	a2r.Call(c, third.ThirdClient.PartLimit, o.Client)
}

func (o *ThirdApi) PartSize(c *gin.Context) {
	a2r.Call(c, third.ThirdClient.PartSize, o.Client)
}

func (o *ThirdApi) InitiateMultipartUpload(c *gin.Context) {
	opt := setURLPrefixOption(third.ThirdClient.InitiateMultipartUpload, func(req *third.InitiateMultipartUploadReq) error {
		return setURLPrefix(c, &req.UrlPrefix)
	})
	a2r.Call(c, third.ThirdClient.InitiateMultipartUpload, o.Client, opt)
}

func (o *ThirdApi) AuthSign(c *gin.Context) {
	a2r.Call(c, third.ThirdClient.AuthSign, o.Client)
}

func (o *ThirdApi) CompleteMultipartUpload(c *gin.Context) {
	opt := setURLPrefixOption(third.ThirdClient.CompleteMultipartUpload, func(req *third.CompleteMultipartUploadReq) error {
		return setURLPrefix(c, &req.UrlPrefix)
	})
	a2r.Call(c, third.ThirdClient.CompleteMultipartUpload, o.Client, opt)
}

func (o *ThirdApi) AccessURL(c *gin.Context) {
	a2r.Call(c, third.ThirdClient.AccessURL, o.Client)
}

func (o *ThirdApi) InitiateFormData(c *gin.Context) {
	a2r.Call(c, third.ThirdClient.InitiateFormData, o.Client)
}

func (o *ThirdApi) CompleteFormData(c *gin.Context) {
	opt := setURLPrefixOption(third.ThirdClient.CompleteFormData, func(req *third.CompleteFormDataReq) error {
		return setURLPrefix(c, &req.UrlPrefix)
	})
	a2r.Call(c, third.ThirdClient.CompleteFormData, o.Client, opt)
}

type objectUploadResp struct {
	URL         string `json:"url"`
	Name        string `json:"name"`
	Size        int64  `json:"size"`
	ContentType string `json:"contentType"`
}

func (o *ThirdApi) UploadObject(c *gin.Context) {
	file, header, err := firstMultipartFile(c, "file", "data")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"errCode": 1001,
			"errMsg":  err.Error(),
			"data": gin.H{
				"contentType":   c.GetHeader("Content-Type"),
				"contentLength": c.Request.ContentLength,
			},
		})
		return
	}
	defer func() {
		_ = file.Close()
	}()

	content, err := io.ReadAll(file)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"errCode": 1002, "errMsg": err.Error(), "data": nil})
		return
	}
	size := int64(len(content))
	if size <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"errCode": 1003, "errMsg": "file is empty", "data": nil})
		return
	}

	name := c.PostForm("name")
	if name == "" && header != nil {
		name = header.Filename
	}
	contentType := c.PostForm("contentType")
	if contentType == "" {
		contentType = http.DetectContentType(content)
	}
	cause := c.PostForm("cause")
	if cause == "" {
		cause = "api-upload"
	}

	ctx := rpcContextFromGin(c)
	partSizeResp, err := o.Client.PartSize(ctx, &third.PartSizeReq{Size: size})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"errCode": 1004, "errMsg": err.Error(), "data": nil})
		return
	}
	partSize := partSizeResp.GetSize()
	partMd5s, partSizes, err := splitPartMD5(content, partSize)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"errCode": 1005, "errMsg": err.Error(), "data": nil})
		return
	}
	hash := aggregatePartMD5(partMd5s)
	urlPrefix := ""
	if err := setURLPrefix(c, &urlPrefix); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"errCode": 1006, "errMsg": err.Error(), "data": nil})
		return
	}

	initResp, err := o.Client.InitiateMultipartUpload(ctx, &third.InitiateMultipartUploadReq{
		Hash:        hash,
		Size:        size,
		PartSize:    partSize,
		MaxParts:    int32(len(partMd5s)),
		Cause:       cause,
		Name:        name,
		ContentType: contentType,
		UrlPrefix:   urlPrefix,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"errCode": 1007, "errMsg": err.Error(), "data": nil})
		return
	}
	if initResp.GetUpload() == nil {
		c.JSON(http.StatusOK, gin.H{"errCode": 0, "errMsg": "", "data": objectUploadResp{URL: initResp.GetUrl(), Name: name, Size: size, ContentType: contentType}})
		return
	}

	uploadParts := signedPartsByNumber(initResp.GetUpload().GetSign())
	offset := int64(0)
	for i, currentPartSize := range partSizes {
		partNumber := int32(i + 1)
		part := uploadParts[partNumber]
		if part == nil {
			c.JSON(http.StatusInternalServerError, gin.H{"errCode": 1008, "errMsg": fmt.Sprintf("missing upload sign for part %d", partNumber), "data": nil})
			return
		}
		partContent := content[offset : offset+currentPartSize]
		offset += currentPartSize
		if err := putSignedPart(ctx, http.DefaultClient, initResp.GetUpload().GetSign(), part, bytes.NewReader(partContent), currentPartSize, contentType); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"errCode": 1009, "errMsg": err.Error(), "data": nil})
			return
		}
	}

	completeResp, err := o.Client.CompleteMultipartUpload(ctx, &third.CompleteMultipartUploadReq{
		UploadID:    initResp.GetUpload().GetUploadID(),
		Parts:       partMd5s,
		Name:        name,
		ContentType: contentType,
		Cause:       cause,
		UrlPrefix:   urlPrefix,
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"errCode": 1010, "errMsg": err.Error(), "data": nil})
		return
	}
	c.JSON(http.StatusOK, gin.H{"errCode": 0, "errMsg": "", "data": objectUploadResp{URL: completeResp.GetUrl(), Name: name, Size: size, ContentType: contentType}})
}

func firstMultipartFile(c *gin.Context, fields ...string) (multipart.File, *multipart.FileHeader, error) {
	if err := c.Request.ParseMultipartForm(32 << 20); err != nil {
		return nil, nil, fmt.Errorf("parse multipart form failed: %w", err)
	}
	for _, field := range fields {
		file, header, err := c.Request.FormFile(field)
		if err == nil {
			return file, header, nil
		}
	}
	return nil, nil, fmt.Errorf("file is required; fields=%v contentType=%q contentLength=%d", fields, c.GetHeader("Content-Type"), c.Request.ContentLength)
}

func rpcContextFromGin(c *gin.Context) context.Context {
	ctx := c.Request.Context()
	if operationID := c.GetString(constant.OperationID); operationID != "" {
		ctx = mcontext.SetOperationID(ctx, operationID)
	}
	if opUserID := c.GetString(constant.OpUserID); opUserID != "" {
		ctx = mcontext.SetOpUserID(ctx, opUserID)
	}
	if platform := c.GetString(constant.OpUserPlatform); platform != "" {
		ctx = mcontext.WithOpUserPlatformContext(ctx, platform)
	}
	return ctx
}

func splitPartMD5(content []byte, partSize int64) ([]string, []int64, error) {
	if partSize <= 0 {
		return nil, nil, fmt.Errorf("invalid part size %d", partSize)
	}
	partNum := len(content) / int(partSize)
	if len(content)%int(partSize) != 0 {
		partNum++
	}
	partMd5s := make([]string, 0, partNum)
	partSizes := make([]int64, 0, partNum)
	for offset := 0; offset < len(content); offset += int(partSize) {
		end := offset + int(partSize)
		if end > len(content) {
			end = len(content)
		}
		sum := md5.Sum(content[offset:end])
		partMd5s = append(partMd5s, hex.EncodeToString(sum[:]))
		partSizes = append(partSizes, int64(end-offset))
	}
	return partMd5s, partSizes, nil
}

func aggregatePartMD5(parts []string) string {
	sum := md5.Sum([]byte(strings.Join(parts, ",")))
	return hex.EncodeToString(sum[:])
}

func signedPartsByNumber(sign *third.AuthSignParts) map[int32]*third.SignPart {
	parts := make(map[int32]*third.SignPart)
	if sign == nil {
		return parts
	}
	for _, part := range sign.GetParts() {
		parts[part.GetPartNumber()] = part
	}
	return parts
}

func putSignedPart(ctx context.Context, client *http.Client, sign *third.AuthSignParts, part *third.SignPart, reader io.Reader, size int64, contentType string) error {
	rawURL := part.GetUrl()
	if rawURL == "" && sign != nil {
		rawURL = sign.GetUrl()
	}
	if rawURL == "" {
		return fmt.Errorf("empty upload url for part %d", part.GetPartNumber())
	}
	if sign != nil && (len(sign.GetQuery())+len(part.GetQuery()) > 0) {
		u, err := url.Parse(rawURL)
		if err != nil {
			return err
		}
		query := u.Query()
		for _, kv := range sign.GetQuery() {
			query[kv.GetKey()] = kv.GetValues()
		}
		for _, kv := range part.GetQuery() {
			query[kv.GetKey()] = kv.GetValues()
		}
		u.RawQuery = query.Encode()
		rawURL = u.String()
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, rawURL, reader)
	if err != nil {
		return err
	}
	if sign != nil {
		for _, kv := range sign.GetHeader() {
			req.Header[kv.GetKey()] = kv.GetValues()
		}
	}
	for _, kv := range part.GetHeader() {
		req.Header[kv.GetKey()] = kv.GetValues()
	}
	if contentType != "" && req.Header.Get("Content-Type") == "" {
		req.Header.Set("Content-Type", contentType)
	}
	req.ContentLength = size
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("put object part %d failed, status %d, body %s", part.GetPartNumber(), resp.StatusCode, string(body))
	}
	return nil
}

func (o *ThirdApi) ObjectRedirect(c *gin.Context) {
	name := c.Param("name")
	if name == "" {
		c.String(http.StatusBadRequest, "name is empty")
		return
	}
	if name[0] == '/' {
		name = name[1:]
	}
	operationID := c.Query("operationID")
	if operationID == "" {
		operationID = strconv.Itoa(rand.Int())
	}
	ctx := mcontext.SetOperationID(c, operationID)
	query := make(map[string]string)
	for key, values := range c.Request.URL.Query() {
		if len(values) == 0 {
			continue
		}
		query[key] = values[0]
	}
	resp, err := o.Client.AccessURL(ctx, &third.AccessURLReq{Name: name, Query: query})
	if err != nil {
		if errs.ErrArgs.Is(err) {
			c.String(http.StatusBadRequest, err.Error())
			return
		}
		if errs.ErrRecordNotFound.Is(err) {
			c.String(http.StatusNotFound, err.Error())
			return
		}
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, resp.Url, nil)
	if err != nil {
		c.String(http.StatusInternalServerError, err.Error())
		return
	}
	for _, key := range []string{"Range", "If-Range", "If-None-Match", "If-Modified-Since"} {
		if value := c.GetHeader(key); value != "" {
			req.Header.Set(key, value)
		}
	}
	objectResp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.String(http.StatusBadGateway, err.Error())
		return
	}
	defer func() {
		_ = objectResp.Body.Close()
	}()
	for _, key := range []string{"Accept-Ranges", "Cache-Control", "Content-Disposition", "Content-Length", "Content-Range", "Content-Type", "ETag", "Last-Modified"} {
		if value := objectResp.Header.Get(key); value != "" {
			c.Header(key, value)
		}
	}
	c.Status(objectResp.StatusCode)
	_, _ = io.Copy(c.Writer, objectResp.Body)
}

// #################### logs ####################.
func (o *ThirdApi) UploadLogs(c *gin.Context) {
	a2r.Call(c, third.ThirdClient.UploadLogs, o.Client)
}

func (o *ThirdApi) DeleteLogs(c *gin.Context) {
	a2r.Call(c, third.ThirdClient.DeleteLogs, o.Client)
}

func (o *ThirdApi) SearchLogs(c *gin.Context) {
	a2r.Call(c, third.ThirdClient.SearchLogs, o.Client)
}

func (o *ThirdApi) GetPrometheus(c *gin.Context) {
	c.Redirect(http.StatusFound, o.GrafanaUrl)
}
