//@formatter:off
/*
 * CommentAugmentationListener
 * Copyright 2025 Karl Eilebrecht
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"):
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
//@formatter:on

package de.calamanari.adl.sql.cnv;

import java.util.Arrays;

import static de.calamanari.adl.FormatUtils.appendIndentOrWhitespace;

/**
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
public class TestCommentAugmentationListener implements SqlAugmentationListener {

    @Override
    public void handleBeforeScript(StringBuilder sb, SqlConversionProcessContext ctx) {
        sb.append("/* before script */");
        appendIndentOrWhitespace(sb, ctx.getStyle(), 0, true);
    }

    @Override
    public void handleBeforeMainStatement(StringBuilder sb, SqlConversionProcessContext ctx, boolean withClausePresent) {
        appendIndentOrWhitespace(sb, ctx.getStyle(), 0, true);
        sb.append("/* before main statement (with=").append(withClausePresent).append(") */");
        appendIndentOrWhitespace(sb, ctx.getStyle(), 0, true);
    }

    @Override
    public void handleAppendJoinType(StringBuilder sb, SqlConversionProcessContext ctx, String tableOrAliasFrom, String tableOrAliasTo,
            String suggestedJoinType) {
        appendIndentOrWhitespace(sb, ctx.getStyle(), 0, true);
        sb.append("/* join type not adjusted for from: ");
        sb.append(tableOrAliasFrom);
        sb.append(" to: ");
        sb.append(tableOrAliasTo);
        sb.append(" */");
        appendIndentOrWhitespace(sb, ctx.getStyle(), 0, true);
        sb.append(suggestedJoinType);
    }

    @Override
    public void handleBeforeOnConditions(StringBuilder sb, SqlConversionProcessContext ctx, String tableOrAliasFrom, String tableOrAliasTo) {
        appendIndentOrWhitespace(sb, ctx.getStyle(), 1, true);
        sb.append("/* before on-conditions from: ").append(tableOrAliasFrom).append(" to: ").append(tableOrAliasTo).append(" */");
        appendIndentOrWhitespace(sb, ctx.getStyle(), 1, true);
    }

    @Override
    public void handleAfterOnConditions(StringBuilder sb, SqlConversionProcessContext ctx, String tableOrAliasFrom, String tableOrAliasTo) {
        appendIndentOrWhitespace(sb, ctx.getStyle(), 1, true);
        sb.append("/* after on-conditions from: ").append(tableOrAliasFrom).append(" to: ").append(tableOrAliasTo).append(" */");
        appendIndentOrWhitespace(sb, ctx.getStyle(), 1, true);
    }

    @Override
    public void handleAfterScript(StringBuilder sb, SqlConversionProcessContext ctx) {
        appendIndentOrWhitespace(sb, ctx.getStyle(), 0, true);
        sb.append("/* after script */");
    }

    @Override
    public void handleBeforeOnClause(StringBuilder sb, SqlConversionProcessContext ctx, String tableOrAliasFrom, String tableOrAliasTo) {
        appendIndentOrWhitespace(sb, ctx.getStyle(), 1, true);
        sb.append("/* before on clause with tables: ").append(tableOrAliasFrom).append(", ").append(tableOrAliasTo).append(" */");
        appendIndentOrWhitespace(sb, ctx.getStyle(), 1, true);
    }

    @Override
    public void handleAfterMainSelect(StringBuilder sb, SqlConversionProcessContext ctx) {
        appendIndentOrWhitespace(sb, ctx.getStyle(), 0, true);
        sb.append("/* after main SELECT */");
        appendIndentOrWhitespace(sb, ctx.getStyle(), 0, true);
    }

    @Override
    public void handleAfterWithSelect(StringBuilder sb, SqlConversionProcessContext ctx, String... tables) {
        appendIndentOrWhitespace(sb, ctx.getStyle(), 0, true);
        sb.append("/* after WITH-SELECT tables: ").append(Arrays.toString(tables)).append(" */");
        appendIndentOrWhitespace(sb, ctx.getStyle(), 0, true);
    }

}
