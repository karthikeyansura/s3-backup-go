// s3backup performs full or incremental backups of a directory hierarchy
// to a single S3 object using a log-structured filesystem format.
//
// Usage:
//
//	s3backup --bucket BUCKET [--incremental OBJECT] [--max SIZE] OBJECT /path
//
// Environment variables: S3_HOSTNAME, S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/karthikeyansura/s3-backup-go/pkg/backup"
	"github.com/karthikeyansura/s3-backup-go/pkg/store"
)

func main() {
	var (
		bucket      string
		incremental string
		maxSize     string
		hostname    string
		accessKey   string
		secretKey   string
		protocol    string
		tag         string
		local       bool
		verbose     bool
		noio        bool
		exclude     []string
	)

	rootCmd := &cobra.Command{
		Use:   "s3backup [flags] OBJECT DIRECTORY",
		Short: "Backup a directory tree to a single S3 object",
		Long: `s3backup stores a snapshot of a filesystem as a single S3 object,
using a simplified log-structured filesystem. It supports incremental
backups by chaining a sequence of these objects.`,
		Args: cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			objectName := args[0]
			dirPath := args[1]

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
			if local {
				st = store.NewLocalStore()
			} else {
				useSSL := protocol != "http"
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

			// Parse --max size
			var stopAfterSectors int64
			if maxSize != "" {
				bytes, err := backup.ParseSize(maxSize)
				if err != nil {
					return fmt.Errorf("invalid --max: %w", err)
				}
				stopAfterSectors = bytes / 512
			}

			// Determine version index
			// For a full backup: versionIdx = 0
			// For incremental: versionIdx = nvers of old backup
			versionIdx := 0
			if incremental != "" {
				ctx := context.Background()
				sbData, err := st.GetRange(ctx, incremental, 0, 20)
				if err != nil {
					return fmt.Errorf("read old superblock: %w", err)
				}
				if len(sbData) >= 20 {
					// nvers is at offset 16, uint32 LE
					versionIdx = int(uint32(sbData[16]) | uint32(sbData[17])<<8 |
						uint32(sbData[18])<<16 | uint32(sbData[19])<<24)
				}
			}

			cfg := backup.Config{
				Store:      st,
				Bucket:     bucket,
				NewName:    objectName,
				OldName:    incremental,
				Dir:        dirPath,
				Tag:        tag,
				Verbose:    verbose,
				NoIO:       noio,
				Exclude:    exclude,
				StopAfter:  stopAfterSectors,
				VersionIdx: versionIdx,
			}

			result, err := backup.Run(context.Background(), cfg)
			if err != nil {
				return err
			}

			fmt.Printf("%d files (%d sectors)\n", result.Stats.Files, result.Stats.FileSectors)
			fmt.Printf("%d directories (%d sectors, %d bytes)\n",
				result.Stats.Dirs, result.Stats.DirSectors, result.Stats.DirBytes)
			fmt.Printf("%d symlinks\n", result.Stats.Symlinks)
			fmt.Printf("%d total sectors\n", result.Stats.TotalSectors)
			if result.Truncated {
				fmt.Println("truncated: YES")
			}

			return nil
		},
	}

	flags := rootCmd.Flags()
	flags.StringVarP(&bucket, "bucket", "b", "", "S3 bucket name (required)")
	flags.StringVarP(&incremental, "incremental", "i", "", "Previous backup object for incremental")
	flags.StringVarP(&maxSize, "max", "m", "", "Stop after SIZE bytes (K/M/G suffixes)")
	flags.StringVar(&hostname, "hostname", "", "S3 hostname (or S3_HOSTNAME env)")
	flags.StringVar(&accessKey, "access-key", "", "S3 access key (or S3_ACCESS_KEY_ID env)")
	flags.StringVar(&secretKey, "secret-key", "", "S3 secret key (or S3_SECRET_ACCESS_KEY env)")
	flags.StringVarP(&protocol, "protocol", "p", "https", "S3 protocol (http/https)")
	flags.StringVarP(&tag, "tag", "t", "--root--", "Tag for root entry")
	flags.BoolVarP(&local, "local", "l", false, "Use local files instead of S3")
	flags.BoolVarP(&verbose, "verbose", "v", false, "Verbose output")
	flags.BoolVarP(&noio, "noio", "n", false, "No output (test only)")
	flags.StringSliceVarP(&exclude, "exclude", "e", nil, "Exclude paths")

	_ = rootCmd.MarkFlagRequired("bucket")

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
