package org.apache.flink.table.catalog.glue.constants;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.catalog.glue.GlueCatalog;

/** Defaults for {@link GlueCatalog}. */
@PublicEvolving
public class GlueCatalogConfigConstants {

    public static final String BASE_GLUE_USER_AGENT_PREFIX_FORMAT =
            "Apache Flink %s (%s) Glue Catalog";

    /** Glue Catalog identifier for user agent prefix. */
    public static final String GLUE_CLIENT_USER_AGENT_PREFIX = "aws.glue.client.user-agent-prefix";
}
