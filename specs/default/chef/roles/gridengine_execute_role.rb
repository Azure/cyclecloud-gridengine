# Copyright (c) Microsoft Corporation. All rights reserved.
# Licensed under the MIT License.
name "gridengine_execute_role"
description "GridEngine Client Role"
run_list( "recipe[cshared::client]",
          "recipe[cuser]",
          "recipe[gridengine::execute]")
