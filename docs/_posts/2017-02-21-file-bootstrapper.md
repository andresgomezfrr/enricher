---
layout: page
title: "File Bootstrapper"
category: bootstrapper
date: 2017-02-21 10:23:07
order: 2
---

`io.wizzie.bootstrapper.bootstrappers.impl.FileBootstrapper`

This bootstrapper reads the stream config from local file system, and builds a KS topology using this file. You need to add the properties on the configuration file.

| Property     | Description     | 
| :------------- | :-------------  | 
| `file.bootstrapper.path`      | Stream config file path      |

Library: https://github.com/wizzie-io/config-bootstrapper