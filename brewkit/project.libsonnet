local images = import "images.libsonnet";

local cache = std.native("cache");
local copy = std.native("copy");
local copyFrom = std.native("copyFrom");

// External cache for go compiler, go mod, golangci-lint
local gocache = [
    cache("go-build", "/app/cache"),
    cache("go-mod", "/go/pkg/mod"),
];

// Sources which will be tracked for changes
local gosources = [
    "go.mod",
    "go.sum",
    "pkg",
];

{
    // Function that generate project build definitions, including code generating, app compilation and e.t.c
    project():: {
        apiVersion: "brewkit/v1",

        targets: {
            all: ["modules", "test", "check"],

            gobase: {
                from: images.gobuilder,
                workdir: "/app",
                env: {
                    GOCACHE: "/app/cache/go-build",
                    CGO_ENABLED: "0",
                },
                copy: copyFrom(
                    "gosources",
                    "/app",
                    "/app"
                ),
            },
        } + {
            gosources: {
                from: "scratch",
                workdir: "/app",
                copy: [copy(source, source) for source in gosources]
            },

            modules: ["gotidy", "modulesvendor"],

            gotidy: {
                from: "gobase",
                workdir: "/app",
                cache: gocache,
                ssh: {},
                command: "
                    go mod tidy
                ",
                output: {
                    artifact: "/app/go.*",
                    "local": ".",
                },
            },

            check: {
                from: images.golangcilint,
                workdir: "/app",
                cache: gocache,
                env: {
                    GOCACHE: "/app/cache/go-build",
                    GOLANGCI_LINT_CACHE: "/app/cache/go-build",
                },
                copy: [
                    copy(".golangci.yml", ".golangci.yml"),
                    copyFrom(
                        "gosources",
                        "/app",
                        "/app"
                    ),
                ],
                command: "golangci-lint run",
            },

            // export local copy of dependencies for ide index
            modulesvendor: {
                from: "gotidy",
                workdir: "/app",
                cache: gocache,
                command: "go mod vendor",
                output: {
                    artifact: "/app/vendor",
                    "local": "vendor",
                },
            },

            test: {
                from: "gobase",
                workdir: "/app",
                cache: gocache,
                command: "go test ./...",
            },
        },
    },
}
