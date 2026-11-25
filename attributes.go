package gofakes3

import (
	"encoding/hex"
	"net/http"
	"strconv"
	"strings"

	xml "github.com/minio/xxml"
)

// GetObjectAttributesResult is the response to GetObjectAttributes.
type GetObjectAttributesResult struct {
	XMLName      xml.Name                  `xml:"GetObjectAttributesResponse"`
	Xmlns        string                    `xml:"xmlns,attr"`
	ETag         string                    `xml:"ETag,omitempty"`
	Checksum     *ObjectAttributesChecksum `xml:"Checksum,omitempty"`
	ObjectParts  *ObjectAttributesParts    `xml:"ObjectParts,omitempty"`
	StorageClass string                    `xml:"StorageClass,omitempty"`
	ObjectSize   *int64                    `xml:"ObjectSize,omitempty"`
}

// ObjectAttributesChecksum is the checksum of an object in a
// GetObjectAttributesResult.
type ObjectAttributesChecksum struct {
	ChecksumCRC32     string `xml:"ChecksumCRC32,omitempty"`
	ChecksumCRC32C    string `xml:"ChecksumCRC32C,omitempty"`
	ChecksumCRC64NVME string `xml:"ChecksumCRC64NVME,omitempty"`
	ChecksumSHA1      string `xml:"ChecksumSHA1,omitempty"`
	ChecksumSHA256    string `xml:"ChecksumSHA256,omitempty"`
	ChecksumType      string `xml:"ChecksumType,omitempty"`
}

// ObjectAttributesParts is the page of the parts of a multipart object
// in a GetObjectAttributesResult.
type ObjectAttributesParts struct {
	PartsCount           int                    `xml:"PartsCount"`
	PartNumberMarker     int                    `xml:"PartNumberMarker"`
	NextPartNumberMarker int                    `xml:"NextPartNumberMarker"`
	MaxParts             int                    `xml:"MaxParts"`
	IsTruncated          bool                   `xml:"IsTruncated"`
	Parts                []ObjectAttributesPart `xml:"Part"`
}

// ObjectAttributesPart is one part of a multipart object in a
// GetObjectAttributesResult.
type ObjectAttributesPart struct {
	PartNumber int   `xml:"PartNumber"`
	Size       int64 `xml:"Size"`
}

// objectPart returns the part of the object to return for a GET or
// HEAD with partNumber in the query of r, and the number of parts the
// object has. part is nil for the whole object, which is part 1 of an
// object not made by a multipart upload.
func (g *GoFakeS3) objectPart(r *http.Request, bucket, object string) (part *ObjectRangeRequest, partsCount int, err error) {
	v := r.URL.Query().Get("partNumber")
	if v == "" {
		return nil, 0, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 || n > MaxUploadPartNumber {
		return nil, 0, ErrorInvalidArgument("partNumber", v, "Part number must be an integer between 1 and 10000, inclusive")
	}
	if r.Header.Get("Range") != "" {
		return nil, 0, ErrorMessage(ErrInvalidRequest, "Cannot specify both Range header and partNumber query parameter")
	}
	obj, err := g.storage.HeadObject(r.Context(), bucket, object)
	if err != nil {
		return nil, 0, err
	}
	if obj.Contents != nil {
		_ = obj.Contents.Close()
	}
	if len(obj.PartSizes) == 0 {
		if n == 1 {
			return nil, 0, nil
		}
		return nil, 0, ErrorMessage(ErrInvalidPart, "The requested partnumber is not satisfiable")
	}
	if n > len(obj.PartSizes) {
		return nil, 0, ErrorMessage(ErrInvalidPart, "The requested partnumber is not satisfiable")
	}
	var start int64
	for _, size := range obj.PartSizes[:n-1] {
		start += size
	}
	return &ObjectRangeRequest{Start: start, End: start + obj.PartSizes[n-1] - 1}, len(obj.PartSizes), nil
}

// objectAttributesChecksum returns the stored checksum of obj, or nil
// if it doesn't have one.
func objectAttributesChecksum(obj *Object) *ObjectAttributesChecksum {
	var c ObjectAttributesChecksum
	found := false
	for k, v := range obj.Metadata {
		if !strings.HasPrefix(k, checksumHeaderPrefix) {
			continue
		}
		found = true
		switch strings.ToLower(strings.TrimPrefix(k, checksumHeaderPrefix)) {
		case "crc32":
			c.ChecksumCRC32 = v
		case "crc32c":
			c.ChecksumCRC32C = v
		case "crc64nvme":
			c.ChecksumCRC64NVME = v
		case "sha1":
			c.ChecksumSHA1 = v
		case "sha256":
			c.ChecksumSHA256 = v
		default:
			found = false
		}
	}
	if !found {
		return nil
	}
	c.ChecksumType = "FULL_OBJECT"
	return &c
}

// getObjectAttributes returns the attributes of an object named in the
// x-amz-object-attributes header.
func (g *GoFakeS3) getObjectAttributes(bucket, object string, w http.ResponseWriter, r *http.Request) error {
	g.log.Print(LogInfo, "GET OBJECT ATTRIBUTES", bucket, object)
	if err := g.ensureBucketExists(r, bucket); err != nil {
		return err
	}

	attributes := map[string]bool{}
	for _, v := range r.Header.Values("X-Amz-Object-Attributes") {
		for _, attribute := range strings.Split(v, ",") {
			attributes[strings.TrimSpace(attribute)] = true
		}
	}
	if len(attributes) == 0 {
		return ErrorInvalidArgument("x-amz-object-attributes", "", "Invalid attribute name specified.")
	}
	maxParts, err := parseClampedInt(r.Header.Get("X-Amz-Max-Parts"), DefaultMaxUploadParts, 0, MaxUploadPartsLimit)
	if err != nil {
		return ErrorInvalidArgument("x-amz-max-parts", r.Header.Get("X-Amz-Max-Parts"), "Argument max-parts must be an integer between 0 and 1000")
	}
	marker, err := parseClampedInt(r.Header.Get("X-Amz-Part-Number-Marker"), 0, 0, MaxUploadPartNumber)
	if err != nil {
		return ErrorInvalidArgument("x-amz-part-number-marker", r.Header.Get("X-Amz-Part-Number-Marker"), "Argument part-number-marker must be an integer between 0 and 10000")
	}

	obj, err := g.storage.HeadObject(r.Context(), bucket, object)
	if err != nil {
		return err
	}
	if obj.Contents != nil {
		_ = obj.Contents.Close()
	}

	result := GetObjectAttributesResult{Xmlns: "http://s3.amazonaws.com/doc/2006-03-01/"}
	if attributes["ETag"] {
		result.ETag = hex.EncodeToString(obj.Hash)
	}
	if attributes["Checksum"] {
		result.Checksum = objectAttributesChecksum(obj)
	}
	if attributes["StorageClass"] {
		result.StorageClass = "STANDARD"
	}
	if attributes["ObjectSize"] {
		result.ObjectSize = &obj.Size
	}
	if attributes["ObjectParts"] && len(obj.PartSizes) > 0 {
		parts := &ObjectAttributesParts{
			PartsCount:       len(obj.PartSizes),
			PartNumberMarker: int(marker),
			MaxParts:         int(maxParts),
		}
		for i := int(marker); i < len(obj.PartSizes); i++ {
			if len(parts.Parts) == parts.MaxParts {
				parts.IsTruncated = true
				break
			}
			parts.Parts = append(parts.Parts, ObjectAttributesPart{PartNumber: i + 1, Size: obj.PartSizes[i]})
			parts.NextPartNumberMarker = i + 1
		}
		result.ObjectParts = parts
	}

	if lastModified := obj.Metadata["Last-Modified"]; lastModified != "" {
		w.Header().Set("Last-Modified", lastModified)
	}
	return g.xmlEncoder(w).Encode(result)
}
