package cloudinary

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/cloudinary/cloudinary-go/v2"
	"github.com/cloudinary/cloudinary-go/v2/api"
	"github.com/cloudinary/cloudinary-go/v2/api/admin"
	"github.com/cloudinary/cloudinary-go/v2/api/admin/search"
	"github.com/cloudinary/cloudinary-go/v2/api/uploader"
	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fs/config"
	"github.com/rclone/rclone/fs/config/configmap"
	"github.com/rclone/rclone/fs/config/configstruct"
	"github.com/rclone/rclone/fs/fshttp"
	"github.com/rclone/rclone/fs/hash"
	"github.com/rclone/rclone/fs/object"
	"github.com/rclone/rclone/lib/encoder"
	"github.com/rclone/rclone/lib/rest"
)

// Extend the built-in eccoder
type CloudinaryEncoder interface {
	// FromStandardPath takes a / separated path in Standard encoding
	// and converts it to a / separated path in this encoding.
	FromStandardPath(string) string
	// FromStandardName takes name in Standard encoding and converts
	// it in this encoding.
	FromStandardName(string) string
	// ToStandardPath takes a / separated path in this encoding
	// and converts it to a / separated path in Standard encoding.
	ToStandardPath(string) string
	// ToStandardName takes name in this encoding and converts
	// it in Standard encoding.
	ToStandardName(string) string
}

func (f *Fs) FromStandardPath(s string) string {
	return strings.Replace(f.opt.Enc.FromStandardPath(s), "&", "\uFF06", -1)
}

func (f *Fs) FromStandardName(s string) string {
	return strings.Replace(f.opt.Enc.FromStandardName(s), "&", "\uFF06", -1)
}

func (f *Fs) ToStandardPath(s string) string {
	return strings.Replace(f.opt.Enc.ToStandardPath(s), "\uFF06", "&", -1)
}

func (f *Fs) ToStandardName(s string) string {
	return strings.Replace(f.opt.Enc.ToStandardName(s), "\uFF06", "&", -1)
}

// Cloudinary shouldn't have a trailing dot if there is no path
func cldPathDir(somePath string) string {
	if somePath == "" || somePath == "." {
		return somePath
	}
	dir := path.Dir(somePath)
	if dir == "." {
		return ""
	}
	return dir
}

// Register with Fs
func init() {
	fs.Register(&fs.RegInfo{
		Name:        "cloudinary",
		Description: "Cloudinary",
		NewFs:       NewFs,
		Options: []fs.Option{
			{
				Name:     "cloud_name",
				Help:     "Cloudinary Environment Name",
				Required: true,
			},
			{
				Name:     "api_key",
				Help:     "Cloudinary API Key",
				Required: true,
			},
			{
				Name:       "api_secret",
				Help:       "Cloudinary API Secret",
				Required:   true,
				IsPassword: true,
			},
			{
				Name: "upload_preset",
				Help: "Upload Preset to select asset manipulation on upload",
			},
			{
				Name:     config.ConfigEncoding,
				Help:     config.ConfigEncodingHelp,
				Advanced: true,
				Default: (encoder.Base | //  Slash,LtGt,DoubleQuote,Question,Asterisk,Pipe,Hash,Percent,BackSlash,Del,Ctl,RightSpace,InvalidUtf8,Dot
					encoder.EncodeSlash |
					encoder.EncodeLtGt |
					encoder.EncodeDoubleQuote |
					encoder.EncodeQuestion |
					encoder.EncodeAsterisk |
					encoder.EncodePipe |
					encoder.EncodeHash |
					encoder.EncodePercent |
					encoder.EncodeBackSlash |
					encoder.EncodeDel |
					encoder.EncodeCtl |
					encoder.EncodeRightSpace |
					encoder.EncodeInvalidUtf8 |
					encoder.EncodeDot),
			},
			{
				Name:     "optimistic_search",
				Default:  false,
				Advanced: true,
				Help:     "Assume the asset is there so will retry Search",
			},
		},
	})
}

// Options defines the configuration for this backend
type Options struct {
	CloudName        string               `config:"cloud_name"`
	APIKey           string               `config:"api_key"`
	APISecret        string               `config:"api_secret"`
	UploadPreset     string               `config:"upload_preset"`
	Enc              encoder.MultiEncoder `config:"encoding"`
	OptimisticSearch bool                 `config:"optimistic_search"`
}

// Fs represents a remote cloudinary server
type Fs struct {
	name     string
	root     string
	opt      Options
	features *fs.Features
	srv      *rest.Client // the connection to the server
	cld      *cloudinary.Cloudinary
}

// Object describes a cloudinary object
type Object struct {
	fs      *Fs
	remote  string
	size    int64
	modTime time.Time
	url     string
	md5sum  string
}

type UpdateModeOption struct {
	UpdateMode bool
}

func (o *UpdateModeOption) Header() (string, string) {
	return "UpdateMode", fmt.Sprintf("%v", o.UpdateMode)
}

// Mandatory returns whether the option must be parsed or can be ignored
func (o *UpdateModeOption) Mandatory() bool {
	return false
}

// String formats the option into human-readable form
func (o *UpdateModeOption) String() string {
	return fmt.Sprintf("UpdateModeOption(%v)", o.UpdateMode)
}

func (o *Object) Hash(ctx context.Context, ty hash.Type) (string, error) {
	if ty != hash.MD5 {
		return "", hash.ErrUnsupported
	}
	return o.md5sum, nil
}

func (o *Object) String() string {
	if o == nil {
		return "<nil>"
	}
	return o.remote
}

// NewFs constructs an Fs from the path, bucket:path
func NewFs(ctx context.Context, name string, root string, m configmap.Mapper) (fs.Fs, error) {
	opt := new(Options)
	err := configstruct.Set(m, opt)
	if err != nil {
		return nil, err
	}

	// Initialize the Cloudinary client
	cld, err := cloudinary.NewFromParams(opt.CloudName, opt.APIKey, opt.APISecret)
	if err != nil {
		return nil, fmt.Errorf("failed to create Cloudinary client: %w", err)
	}
	client := fshttp.NewClient(ctx)
	f := &Fs{
		name: name,
		root: root,
		opt:  *opt,
		cld:  cld,
		srv:  rest.NewClient(client),
	}

	f.features = (&fs.Features{
		CaseInsensitive:         false,
		CanHaveEmptyDirectories: true,
		DuplicateFiles:          true,
	}).Fill(ctx, f)

	if root != "" {
		// Check to see if the root actually an existing file
		remote := path.Base(root)
		f.root = cldPathDir(root)
		_, err := f.NewObject(ctx, remote)
		if err != nil {
			if err == fs.ErrorObjectNotFound || errors.Is(err, fs.ErrorNotAFile) {
				// File doesn't exist so return old f
				f.root = root
				return f, nil
			}
			return nil, err
		}
		// return an error with an fs which points to the parent
		return f, fs.ErrorIsFile
	}
	return f, nil
}

// Name of the remote (as passed into NewFs)
func (f *Fs) Name() string {
	return f.name
}

// Root of the remote (as passed into NewFs)
func (f *Fs) Root() string {
	return f.root
}

// Encoded root of the remote (as passed into NewFs)
func (f *Fs) FromStandardFullPath(dir string) string {
	return path.Join(CloudinaryEncoder.FromStandardPath(f, f.root), CloudinaryEncoder.FromStandardPath(f, dir))
}

// String converts this Fs to a string
func (f *Fs) String() string {
	return fmt.Sprintf("Cloudinary root '%s'", f.root)
}

// Features returns the optional features of this Fs
func (f *Fs) Features() *fs.Features {
	return f.features
}

// List the objects and directories in dir into entries.
func (f *Fs) List(ctx context.Context, dir string) (fs.DirEntries, error) {
	remotePrefix := f.FromStandardFullPath(dir)
	if remotePrefix != "" && !strings.HasSuffix(remotePrefix, "/") {
		remotePrefix += "/"
	}

	var entries fs.DirEntries
	dirs := make(map[string]struct{})
	nextCursor := ""

	for {
		// user the folders api to list folders.
		folderParams := admin.SubFoldersParams{
			Folder:     remotePrefix,
			MaxResults: 500,
		}
		if nextCursor != "" {
			folderParams.NextCursor = nextCursor
		}

		results, err := f.cld.Admin.SubFolders(ctx, folderParams)
		if err != nil {
			return nil, fmt.Errorf("failed to list sub-folders: %w", err)
		}
		if results.Error.Message != "" {
			if strings.HasPrefix(results.Error.Message, "Can't find folder with path") {
				return nil, fs.ErrorDirNotFound
			}

			return nil, fmt.Errorf("failed to list sub-folders: %s", results.Error.Message)
		}

		for _, folder := range results.Folders {
			relativePath := CloudinaryEncoder.ToStandardPath(f, strings.TrimPrefix(folder.Path, remotePrefix))
			parts := strings.Split(relativePath, "/")

			// It's a directory
			dirName := parts[0]
			if _, found := dirs[dirName]; !found {
				d := fs.NewDir(path.Join(dir, dirName), time.Now())
				entries = append(entries, d)
				dirs[dirName] = struct{}{}
			}
		}
		// Break if there are no more results
		if results.NextCursor == "" {
			break
		}
		nextCursor = results.NextCursor
	}

	for {
		// Use the assets.AssetsByAssetFolder API to list assets
		assetsParams := admin.AssetsByAssetFolderParams{
			AssetFolder: remotePrefix,
			MaxResults:  500,
		}
		if nextCursor != "" {
			assetsParams.NextCursor = nextCursor
		}

		results, err := f.cld.Admin.AssetsByAssetFolder(ctx, assetsParams)
		if err != nil {
			return nil, fmt.Errorf("failed to list assets: %w", err)
		}

		for _, asset := range results.Assets {
			remote := CloudinaryEncoder.ToStandardName(f, asset.DisplayName)
			if dir != "" {
				remote = path.Join(dir, CloudinaryEncoder.ToStandardName(f, asset.DisplayName))
			}
			o := &Object{
				fs:      f,
				remote:  remote,
				size:    int64(asset.Bytes),
				modTime: asset.CreatedAt,
				url:     asset.SecureURL,
			}
			entries = append(entries, o)
		}

		// Break if there are no more results
		if results.NextCursor == "" {
			break
		}
		nextCursor = results.NextCursor
	}

	return entries, nil
}

// getCLDAsset finds the asset at Cloudinary. If it can't be found it returns the error fs.ErrorObjectNotFound.
func (f *Fs) getCLDAsset(ctx context.Context, remote string, retry int8) (*admin.SearchAsset, error) {
	// Use the Search API to get the specific asset by display name and asset folder
	searchParams := search.Query{
		Expression: fmt.Sprintf("asset_folder=\"%s\" AND display_name=\"%s\"",
			f.FromStandardFullPath(cldPathDir(remote)),
			CloudinaryEncoder.FromStandardName(f, path.Base(remote))),
		MaxResults: 1,
	}
	results, err := f.cld.Admin.Search(ctx, searchParams)
	if f.opt.OptimisticSearch && len(results.Assets) == 0 && retry < 3 {
		time.Sleep(1 * time.Second)
		return f.getCLDAsset(ctx, remote, retry+1)
	}
	if err != nil || len(results.Assets) == 0 {
		return nil, fs.ErrorObjectNotFound
	}

	if results.NextCursor != "" {
		return nil, errors.New("duplicate objects found")
	}

	return &results.Assets[0], nil
}

// NewObject finds the Object at remote. If it can't be found it returns the error fs.ErrorObjectNotFound.
func (f *Fs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	asset, err := f.getCLDAsset(ctx, remote, 0)
	if err != nil {
		return nil, err
	}

	o := &Object{
		fs:      f,
		remote:  remote,
		size:    int64(asset.Bytes),
		modTime: asset.UploadedAt,
		url:     asset.SecureURL,
		md5sum:  asset.Etag,
	}

	return o, nil
}

// Put uploads content to Cloudinary
func (f *Fs) Put(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) (fs.Object, error) {
	if src.Size() == 0 {
		return nil, fs.ErrorCantUploadEmptyFiles
	}

	params := uploader.UploadParams{
		AssetFolder:  f.FromStandardFullPath(cldPathDir(src.Remote())),
		DisplayName:  CloudinaryEncoder.FromStandardName(f, path.Base(src.Remote())),
		UploadPreset: f.opt.UploadPreset,
	}

	for _, option := range options {
		if updateModeOption, ok := option.(*UpdateModeOption); ok {
			if updateModeOption.UpdateMode {
				params.Overwrite = api.Bool(true)
				params.Invalidate = api.Bool(true)
			}
		}
	}
	params.PublicID = path.Join(params.AssetFolder, params.DisplayName)
	uploadResult, err := f.cld.Upload.Upload(ctx, in, params)
	if err != nil {
		return nil, fmt.Errorf("failed to upload to Cloudinary: %w", err)
	}
	if uploadResult.Error.Message != "" {
		return nil, fmt.Errorf(uploadResult.Error.Message)
	}

	o := &Object{
		fs:      f,
		remote:  src.Remote(),
		size:    int64(uploadResult.Bytes),
		modTime: uploadResult.CreatedAt,
		url:     uploadResult.SecureURL,
		md5sum:  uploadResult.Etag,
	}
	return o, nil
}

// Other required methods (not fully implemented):

func (f *Fs) Precision() time.Duration {
	return fs.ModTimeNotSupported
}

func (f *Fs) Hashes() hash.Set {
	return hash.Set(hash.None)
}

func (f *Fs) Mkdir(ctx context.Context, dir string) error {
	params := admin.CreateFolderParams{Folder: f.FromStandardFullPath(dir)}
	res, err := f.cld.Admin.CreateFolder(ctx, params)
	if err != nil {
		return err
	}
	if res.Error.Message != "" {
		return fmt.Errorf(res.Error.Message)
	}

	return nil
}

func (f *Fs) Rmdir(ctx context.Context, dir string) error {
	params := admin.DeleteFolderParams{Folder: f.FromStandardFullPath(dir)}
	res, err := f.cld.Admin.DeleteFolder(ctx, params)
	if err != nil {
		return err
	}
	if res.Error.Message != "" {
		if strings.HasPrefix(res.Error.Message, "Can't find folder with path") {
			return fs.ErrorDirNotFound
		}

		return fmt.Errorf(res.Error.Message)
	}

	return nil
}

func (f *Fs) Remove(ctx context.Context, o fs.Object) error {
	asset, err := f.getCLDAsset(ctx, o.Remote(), 0)
	if err != nil {
		return err
	}
	params := uploader.DestroyParams{
		PublicID:     asset.PublicID,
		ResourceType: asset.ResourceType,
		Type:         asset.Type,
	}
	res, dErr := f.cld.Upload.Destroy(ctx, params)
	if dErr != nil {
		return dErr
	}

	if res.Error.Message != "" {
		return fmt.Errorf(res.Error.Message)
	}

	if res.Result != "ok" {
		return fmt.Errorf(res.Result)
	}

	return nil
}

// Object methods

func (o *Object) Fs() fs.Info {
	return o.fs
}

func (o *Object) Remote() string {
	return o.remote
}

func (o *Object) ModTime(ctx context.Context) time.Time {
	return o.modTime
}

func (o *Object) Size() int64 {
	return o.size
}

func (o *Object) Storable() bool {
	return true
}

func (o *Object) SetModTime(ctx context.Context, modTime time.Time) error {
	return fs.ErrorCantSetModTime
}

// Open an object for read
func (o *Object) Open(ctx context.Context, options ...fs.OpenOption) (in io.ReadCloser, err error) {
	var resp *http.Response
	opts := rest.Opts{
		Method:  "GET",
		RootURL: o.url,
		Options: options,
	}
	var offset int64
	var count int64
	var key string
	var value string
	fs.FixRangeOption(options, o.size)
	for _, option := range options {
		switch x := option.(type) {
		case *fs.RangeOption:
			offset, count = x.Decode(o.size)
			if count < 0 {
				count = o.size - offset
			}
			key, value = option.Header()
		case *fs.SeekOption:
			offset = x.Offset
			count = o.size - offset
			key, value = option.Header()
		default:
			if option.Mandatory() {
				fs.Logf(o, "Unsupported mandatory option: %v", option)
			}
		}
	}
	if key != "" && value != "" {
		opts.ExtraHeaders = make(map[string]string)
		opts.ExtraHeaders[key] = value
	}
	for i := 1; i <= 7; i++ {
		resp, err = o.fs.srv.Call(ctx, &opts)
		if err != nil {
			return nil, fmt.Errorf("failed download of \"%s\": %w", o.url, err)
		}
		if resp.Header.Get("accept-ranges") != "" && resp.Header.Get("content-length") != "" {
			cl, err := strconv.Atoi(resp.Header.Get("content-length"))
			if err != nil || int64(cl) != count {
				time.Sleep(time.Duration(i) * time.Second)
				continue
			}
			break
		}
		time.Sleep(time.Duration(i) * time.Second)
	}

	return resp.Body, err
}

func (o *Object) Update(ctx context.Context, in io.Reader, src fs.ObjectInfo, options ...fs.OpenOption) error {
	srcImmutable := object.NewStaticObjectInfo(o.Remote(), src.ModTime(ctx), src.Size(), true, nil, o.Fs())
	options = append(options, &UpdateModeOption{UpdateMode: true})
	updatedObj, err := o.fs.Put(ctx, in, srcImmutable, options...)
	if err != nil {
		return err
	}
	if uo, ok := updatedObj.(*Object); ok {
		o.size = uo.size
		o.modTime = time.Now() // Skipping uo.modTime because the API returns the create time
		o.url = uo.url
		o.md5sum = uo.md5sum
	}
	return nil
}

func (o *Object) Remove(ctx context.Context) error {
	return o.fs.Remove(ctx, o)
}
