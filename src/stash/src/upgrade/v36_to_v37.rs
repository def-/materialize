// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::objects::{wire_compatible, WireCompatible};
use crate::upgrade::MigrationAction;
use crate::upgrade::{objects_v36 as v36, objects_v37 as v37};
use crate::{StashError, Transaction, TypedCollection};

wire_compatible!(v36::ItemKey with v37::ItemKey);
wire_compatible!(v36::SchemaId with v37::SchemaId);
wire_compatible!(v36::RoleId with v37::RoleId);
wire_compatible!(v36::MzAclItem with v37::MzAclItem);

const ITEM_COLLECTION: TypedCollection<v36::ItemKey, v36::ItemValue> = TypedCollection::new("item");

/// Persist `false` for existing environments' RBAC flags, iff they're not already set.
pub async fn upgrade(tx: &mut Transaction<'_>) -> Result<(), StashError> {
    ITEM_COLLECTION
        .migrate_to(tx, |entries| {
            entries
                .into_iter()
                .map(|(key, value)| {
                    let new_key: v37::ItemKey = WireCompatible::convert(key);
                    let new_value: v37::ItemValue = value.clone().into();
                    MigrationAction::Update(key.clone(), (new_key, new_value))
                })
                .collect()
        })
        .await
}

impl From<v36::ItemValue> for v37::ItemValue {
    fn from(value: v36::ItemValue) -> Self {
        let create_sql_value = value
            .definition
            .expect("missing field ItemValue::definition")
            .value
            .expect("missing field CatalogItem::value");
        let create_sql = match create_sql_value {
            v36::catalog_item::Value::V1(c) => c.create_sql,
        };
        Self {
            schema_id: value
                .schema_id
                .map(|schema_id| WireCompatible::convert(&schema_id)),
            name: value.name,
            create_sql,
            owner_id: value
                .owner_id
                .map(|owner_id| WireCompatible::convert(&owner_id)),
            privileges: value
                .privileges
                .into_iter()
                .map(|privilege| WireCompatible::convert(&privilege))
                .collect(),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::DebugStashFactory;

    use super::*;

    const ITEM_COLLECTION_V37: TypedCollection<v37::ItemKey, v37::ItemValue> =
        TypedCollection::new("item");

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `TLS_client_method` on OS `linux`
    async fn smoke_test() {
        let factory = DebugStashFactory::new().await;
        let mut stash = factory.open_debug().await;

        ITEM_COLLECTION
            .insert_without_overwrite(
                &mut stash,
                vec![(
                    v36::ItemKey {
                        gid: Some(v36::GlobalId {
                            value: Some(v36::global_id::Value::User(42)),
                        }),
                    },
                    v36::ItemValue {
                        schema_id: Some(v36::SchemaId {
                            value: Some(v36::schema_id::Value::User(66)),
                        }),
                        name: "v".to_string(),
                        definition: Some(v36::CatalogItem {
                            value: Some(v36::catalog_item::Value::V1(v36::catalog_item::V1 {
                                create_sql: "CREATE VIEW v AS SELECT 1".to_string(),
                            })),
                        }),
                        owner_id: Some(v36::RoleId {
                            value: Some(v36::role_id::Value::User(1)),
                        }),
                        privileges: vec![v36::MzAclItem {
                            grantee: Some(v36::RoleId {
                                value: Some(v36::role_id::Value::User(2)),
                            }),
                            grantor: Some(v36::RoleId {
                                value: Some(v36::role_id::Value::User(3)),
                            }),
                            acl_mode: Some(v36::AclMode { bitflags: 55 }),
                        }],
                    },
                )],
            )
            .await
            .unwrap();

        // Run the migration.
        stash
            .with_transaction(|mut tx| {
                Box::pin(async move {
                    upgrade(&mut tx).await?;
                    Ok(())
                })
            })
            .await
            .unwrap();

        let items: Vec<_> = ITEM_COLLECTION_V37
            .peek_one(&mut stash)
            .await
            .unwrap()
            .into_iter()
            .map(|(key, value)| (key, value))
            .collect();

        assert_eq!(
            items,
            vec![(
                v37::ItemKey {
                    gid: Some(v37::GlobalId {
                        value: Some(v37::global_id::Value::User(42)),
                    }),
                },
                v37::ItemValue {
                    schema_id: Some(v37::SchemaId {
                        value: Some(v37::schema_id::Value::User(66)),
                    }),
                    name: "v".to_string(),
                    create_sql: "CREATE VIEW v AS SELECT 1".to_string(),
                    owner_id: Some(v37::RoleId {
                        value: Some(v37::role_id::Value::User(1)),
                    }),
                    privileges: vec![v37::MzAclItem {
                        grantee: Some(v37::RoleId {
                            value: Some(v37::role_id::Value::User(2)),
                        }),
                        grantor: Some(v37::RoleId {
                            value: Some(v37::role_id::Value::User(3)),
                        }),
                        acl_mode: Some(v37::AclMode { bitflags: 55 }),
                    }],
                },
            )],
        );
    }
}
