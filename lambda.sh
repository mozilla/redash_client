#!/usr/bin/env bash

# Path to the project directory (that should include requirements.txt),
# Files and directories within that need to be deployed.
project=.
contents=(redash_client)

# Unnecessary parts. Note that there are some inter-dependencies in SciPy,
# for example to use scipy.stats you also need scipy.linalg, scipy.integrate,
# scipy.misc, scipy.sparse, and scipy.special.
shopt -s globstar
remove=(
    *.*-info **/*.pyc **/__pycache__ **/doc/ **/tests/
    scipy/cluster scipy/constants scipy/fftpack scipy/io
    scipy/ndimage scipy/odr scipy/signal scipy/weave
)

# We'll use a temporary directory to prepare the package contents.
build=$(mktemp -d --suffix=-$redash_client)
function cleanup {
    rm -r "$build"
}
trap cleanup EXIT

# Install the requirements into a directory. As of 2017 Lambda only supports
# Python 2.7... (And calling out to python3 through a subprocess is slow.)
pip2 install -t "$build" -r "$project"/requirements.txt

# Delete unnecessary things and strip libraries to make the archive smaller.
for item in "${remove[@]}"; do
    rm -r "$build"/$item
done
mv "$build"/numpy/.libs/* "$build" && rm -r "$build"/**/.libs
find "$build" -name '*.so' | xargs strip 2> /dev/null

# Copy the application (you may have a path component in your handler
# configuration, for example: backend/lambda.handler).
for item in "${contents[@]}"; do
    cp -r "$project"/$item "$build"
done

# Zip the build directory using high compression.
rm lambda.zip 2> /dev/null
cwd=$(pwd)
cd "$build"
zip -r -9 -q "$cwd"/lambda.zip *
cd "$cwd"

# At the time of writing, the limits are 50 MB (upload) and 250 MB (unpacked).
ls -l lambda.zip
