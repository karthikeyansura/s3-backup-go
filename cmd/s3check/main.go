// s3check validates S3 backup objects and compares them against source trees.
//
// Usage:
//
//	s3check fsck [--local] TARGET
//	s3check diff [--local] TARGET DIRECTORY
package main

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"github.com/karthikeyansura/s3-backup-go/pkg/compare"
	"github.com/karthikeyansura/s3-backup-go/pkg/fsck"
	"github.com/karthikeyansura/s3-backup-go/pkg/mount"
	"github.com/karthikeyansura/s3-backup-go/pkg/store"
)

var (
	hostname  string
	accessKey string
	secretKey string
	local     bool
	useHTTP   bool
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "s3check",
		Short: "Validate and compare s3backup objects",
	}

	pf := rootCmd.PersistentFlags()
	pf.StringVar(&hostname, "hostname", "", "S3 hostname (or S3_HOSTNAME env)")
	pf.StringVar(&accessKey, "access-key", "", "S3 access key (or S3_ACCESS_KEY_ID env)")
	pf.StringVar(&secretKey, "secret-key", "", "S3 secret key (or S3_SECRET_ACCESS_KEY env)")
	pf.BoolVar(&local, "local", false, "Interpret target as a local file path")
	pf.BoolVar(&useHTTP, "http", false, "Use HTTP instead of HTTPS")

	rootCmd.AddCommand(fsckCmd(), diffCmd())

	cobra.OnInitialize(func() {
		if hostname == "" {
			hostname = os.Getenv("S3_HOSTNAME")
		}
		if accessKey == "" {
			accessKey = os.Getenv("S3_ACCESS_KEY_ID")
		}
		if secretKey == "" {
			secretKey = os.Getenv("S3_SECRET_ACCESS_KEY")
		}
	})

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func fsckCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "fsck TARGET",
		Short: "Validate internal consistency of a backup object",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			st, key, err := resolveTarget(args[0])
			if err != nil {
				return err
			}

			rep, err := fsck.Check(context.Background(), st, key)
			if err != nil {
				return err
			}

			fmt.Printf("versions:    %d\n", rep.Versions)
			fmt.Printf("directories: %d\n", rep.Directories)
			fmt.Printf("files:       %d\n", rep.Files)
			fmt.Printf("symlinks:    %d\n", rep.Symlinks)
			fmt.Printf("specials:    %d\n", rep.Specials)

			for _, w := range rep.Warnings {
				fmt.Printf("WARNING: %s\n", w)
			}
			for _, e := range rep.Errors {
				fmt.Printf("ERROR:   %s\n", e)
			}

			if rep.OK() {
				fmt.Println("fsck: OK")
				return nil
			}
			return fmt.Errorf("fsck: FAILED (%d errors)", len(rep.Errors))
		},
	}
}

func diffCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "diff TARGET DIRECTORY",
		Short: "Compare a backup object against a local directory tree",
		Args:  cobra.ExactArgs(2),
		RunE: func(cmd *cobra.Command, args []string) error {
			st, key, err := resolveTarget(args[0])
			if err != nil {
				return err
			}

			ctx := context.Background()
			arch, err := mount.OpenArchive(ctx, st, key, false)
			if err != nil {
				return err
			}
			defer func() { _ = arch.Close() }()

			rep, err := compare.Tree(ctx, arch, args[1])
			if err != nil {
				return err
			}

			fmt.Printf("compared: %d entries\n", rep.Compared)

			for _, p := range rep.MissingInBackup {
				fmt.Printf("MISSING IN BACKUP: %s\n", p)
			}
			for _, p := range rep.MissingInLocal {
				fmt.Printf("MISSING LOCALLY:   %s\n", p)
			}
			for _, m := range rep.Mismatches {
				fmt.Printf("MISMATCH: %s\n", m)
			}

			if rep.OK() {
				fmt.Println("diff: MATCH")
				return nil
			}
			return fmt.Errorf("diff: DIFFER (%d missing in backup, %d missing locally, %d mismatches)",
				len(rep.MissingInBackup), len(rep.MissingInLocal), len(rep.Mismatches))
		},
	}
}

func resolveTarget(target string) (store.ObjectStore, string, error) {
	if local {
		return store.NewLocalStore(), target, nil
	}

	parts := strings.SplitN(target, "/", 2)
	if len(parts) != 2 {
		return nil, "", fmt.Errorf("target must be bucket/key, got %q", target)
	}

	st, err := store.NewS3Store(store.S3Config{
		Endpoint:  hostname,
		AccessKey: accessKey,
		SecretKey: secretKey,
		Bucket:    parts[0],
		UseSSL:    !useHTTP,
	})
	if err != nil {
		return nil, "", err
	}
	return st, parts[1], nil
}
