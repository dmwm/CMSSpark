#!/bin/bash

show_help() {
  cat << EOF
Usage: $0 [options] <image-name> [tag]

Build, tag, and push a container image to registry.cern.ch.

Arguments:
  <image-name>    Name of the image to build (required).
  [tag]           Tag to apply to the image (optional, default: "test").

Options:
  -f, --file      Path to the folder where the Dockerfile is located (optional, default: .)
  -h, --help      Show this help message and exit.

Examples:
  $0 cmsmon-spark
      Builds and pushes the image using ./Dockerfile
      => registry.cern.ch/cmsmonitoring/cmsmon-spark:test

  $0 -f ./cmsmon-spark cmsmon-spark 1.0
      Builds and pushes the image using ./cmsmon-spark/Dockerfile
      => registry.cern.ch/cmsmonitoring/cmsmon-spark:1.0
EOF
}

# defaults
PATH="."

# parse options
while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)
      show_help
      exit 0
      ;;
    -f|--file)
      if [[ -n "$2" ]]; then
        PATH="$2"
        shift
      else
        echo "Error: --file option requires a path argument."
        exit 1
      fi
      ;;
    -*)
      echo "Unknown option: $1"
      echo "Try '$0 --help' for more information."
      exit 1
      ;;
    *)
      # first non-option argument is IMAGE_NAME
      if [ -z "$IMAGE_NAME" ]; then
        IMAGE_NAME="$1"
      elif [ -z "$TAG" ]; then
        TAG="$1"
      else
        echo "Error: too many arguments."
        echo "Try '$0 --help' for more information."
        exit 1
      fi
      ;;
  esac
  shift
done

# validate inputs
if [ -z "$IMAGE_NAME" ]; then
  echo "Error: <image-name> is required."
  echo "Try '$0 --help' for more information."
  exit 1
fi

TAG="${TAG:-test}"

# build, tag, and push
/usr/bin/podman build -f "$PATH/Dockerfile" -t "local/${IMAGE_NAME}" "$PATH"
/usr/bin/podman tag "local/${IMAGE_NAME}" "registry.cern.ch/cmsmonitoring/${IMAGE_NAME}:${TAG}"
/usr/bin/podman push "registry.cern.ch/cmsmonitoring/${IMAGE_NAME}:${TAG}"
