# Copyright Materialize, Inc. and contributors. All rights reserved.
#
# Use of this software is governed by the Business Source License
# included in the LICENSE file at the root of this repository.
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0.

# Test that Kafka sources with no format are disallowed.

> CREATE CONNECTION kafka_conn TO KAFKA (BROKER '${testdrive.kafka-addr}');

! CREATE SOURCE s
  FROM KAFKA CONNECTION kafka_conn (TOPIC 'testdrive-data-${testdrive.seed}')
contains:Source format must be specified

> CREATE CONNECTION no_topic TO KAFKA (BROKER '');

! CREATE SOURCE s
  FROM KAFKA CONNECTION no_topic
contains:KAFKA CONNECTION without TOPIC
