// s3mount mounts an S3 backup object as a read-only FUSE filesystem.
//
// Usage:
//
//	s3mount [flags] bucket/key /mountpoint
//
// Environment variables: S3_HOSTNAME, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/spf13/cobra"

	"github.com/karthikeyansura/s3-backup-go/pkg/mount"
	"github.com/karthikeyansura/s3-backup-go/pkg/store"
)

func main() {
	var (
		hostname  string
		accessKey string
		secretKey string
		local     bool
		useHTTP   bool
		noCache   bool
		verbose   bool
	)

	rootCmd := &cobra.Command{
		Use:   "s3mount [flags] bucket/key /mountpoint",
		Short: "Mount an S3 backup as a read-only FUSE filesystem",
		Long: `s3mount mounts a backup created by s3backup as a read-only FUSE
filesystem. It caches directory data using mmap for low memory usage,
and implements an LRU block cache for file data.`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			target := args[0]
			mountpoint := args[1]

			// Resolve credentials from flags or environment
			if hostname == "" {
				hostname = os.Getenv("S3_HOSTNAME")
			}
			if accessKey == "" {
				accessKey = os.Getenv("S3_ACCESS_KEY_ID")
			}
			if secretKey == "" {
				secretKey = os.Getenv("S3_SECRET_ACCESS_KEY")
			}

			// Create the object store
			var st store.ObjectStore
			var objectKey string

			if local {
				st = store.NewLocalStore()
				objectKey = target
			} else {
				// Parse "bucket/key" format
				parts := strings.SplitN(target, "/", 2)
				if len(parts) != 2 {
					return fmt.Errorf("target must be in 'bucket/key' format, got: %s", target)
				}
				bucket := parts[0]
				objectKey = parts[1]

				useSSL := !useHTTP
				var err error
				st, err = store.NewS3Store(store.S3Config{
					Endpoint:  hostname,
					AccessKey: accessKey,
					SecretKey: secretKey,
					Bucket:    bucket,
					UseSSL:    useSSL,
				})
				if err != nil {
					return fmt.Errorf("failed to initialize S3: %w", err)
				}
			}

			// Verify mountpoint exists
			info, err := os.Stat(mountpoint)
			if err != nil {
				return fmt.Errorf("mountpoint %s: %w", mountpoint, err)
			}
			if !info.IsDir() {
				return fmt.Errorf("mountpoint %s is not a directory", mountpoint)
			}

			ctx := context.Background()

			// Initialize the filesystem
			s3fs, err := mount.NewS3FS(ctx, mount.Config{
				Store:     st,
				ObjectKey: objectKey,
				Verbose:   verbose,
				NoCache:   noCache,
			})
			if err != nil {
				return err
			}
			defer func() { _ = s3fs.Close() }()

			// Mount with go-fuse
			opts := &fs.Options{
				MountOptions: fuse.MountOptions{
					AllowOther:    true,
					FsName:        "s3backup",
					Name:          "s3backup",
					DisableXAttrs: true,
				},
				// Long cache timeouts for read-only FS
				AttrTimeout:  nil, // use defaults set in EntryOut
				EntryTimeout: nil,
			}

			server, err := fs.Mount(mountpoint, s3fs, opts)
			if err != nil {
				return fmt.Errorf("mount failed: %w", err)
			}

			fmt.Printf("Mounted %s at %s (Ctrl+C to unmount)\n", target, mountpoint)

			// Handle signals for clean unmount
			sigCh := make(chan os.Signal, 1)
			signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

			go func() {
				<-sigCh
				fmt.Println("\nUnmounting...")
				_ = server.Unmount()
			}()

			// Serve until unmounted
			server.Wait()

			fmt.Println("Unmounted.")
			return nil
		},
	}

	flags := rootCmd.Flags()
	flags.StringVar(&hostname, "hostname", "", "S3 hostname (or S3_HOSTNAME env)")
	flags.StringVar(&accessKey, "access-key", "", "S3 access key (or S3_ACCESS_KEY_ID env)")
	flags.StringVar(&secretKey, "secret-key", "", "S3 secret key (or S3_SECRET_ACCESS_KEY env)")
	flags.BoolVar(&local, "local", false, "Interpret target as local file path")
	flags.BoolVar(&useHTTP, "http", false, "Use HTTP instead of HTTPS")
	flags.BoolVar(&noCache, "nocache", false, "Disable data block cache")
	flags.BoolVarP(&verbose, "verbose", "v", false, "Verbose output")

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
