# @selfage/gcs_client

## Install

`npm install @selfage/gcs_client`

## Overview

Written in TypeScript and compiled with [@selfage/tsconfig](https://www.npmjs.com/package/@selfage/tsconfig). Provides a client library for a subset APIs of Google Cloud Storage (GCS).

## Features

1. Single chunk upload.
1. Resumable upload, with resumable URL and byteOffset being tracked.

## Test setup

The test is talking to a real GCP project and it's a local unit test.

Prepare for authentication: `gcloud auth application-default login`, or an environment already logged in, such as a Compute Engine.

Make sure the account has suffcient permissions to view/edit/delete objects.

Create a bucket to be used in the test, by setting the environment variable `gcs_bucket=`.
