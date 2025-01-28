//@formatter:off
/*
 * QueryTemplateWithParameters
 * Copyright 2024 Karl Eilebrecht
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

package de.calamanari.adl.sql;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.calamanari.adl.cnv.tps.AdlFormattingException;
import de.calamanari.adl.sql.QueryTemplateParser.ParameterListener;

/**
 * An {@link QueryTemplateWithParameters} combines an SQL-script with <i>positional parameters</i> (question marks) with the parameters to be set once the
 * related {@link PreparedStatement} has been created.
 * 
 * @param qmTemplate template with <b>positional</b> parameters
 * @param orderedParameters parameters <b>in order of appearance in the template</b>
 * @param qmPositions parameter question mark positions <b>in order of appearance in the template</b>
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
public record QueryTemplateWithParameters(String qmTemplate, List<QueryParameter> orderedParameters, List<Integer> qmPositions) implements Serializable {

    private static final long serialVersionUID = 418932408030638050L;

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryTemplateWithParameters.class);

    /**
     * Takes a template with named parameters (<code>${id}</code> references) and a list of parameters to produce a query template with positional parameters to
     * be executed later as a {@link PreparedStatement} after calling {@link #apply(PreparedStatement)} to set the parameters safely.
     * 
     * @param queryTemplate template with <b>named</b> parameters (name=id)
     * @param parameters available parameters, can be a superset of the parameters required by the template
     * @return template with related parameters
     */
    public static QueryTemplateWithParameters of(String queryTemplate, List<QueryParameter> parameters) {
        return new Builder(queryTemplate, parameters).get();
    }

    /**
     * In general you should use {@link #of(String, List)} instead of directly calling this constructor
     * 
     * @param qmTemplate template with <b>positional</b> parameters
     * @param orderedParameters parameters <b>in order of appearance in the template</b>
     * @param qmPositions parameter question mark positions <b>in order of appearance in the template</b>
     */
    public QueryTemplateWithParameters(String qmTemplate, List<QueryParameter> orderedParameters, List<Integer> qmPositions) {
        if (qmTemplate == null || orderedParameters == null || qmPositions == null || containsAnyNull(orderedParameters) || containsAnyNull(qmPositions)) {
            throw new IllegalArgumentException(
                    String.format("Arguments must not be null lists must not contain nulls, given: template=%s, orderedParameters=%s, qmPositions=%s",
                            qmTemplate, orderedParameters, qmPositions));
        }
        if (orderedParameters.size() != qmPositions.size()) {
            throw new IllegalArgumentException(String.format(
                    "The number of question mark positions must be equal to the number of parameters: template=%s, orderedParameters=%s, qmPositions=%s",
                    qmTemplate, orderedParameters, qmPositions));
        }
        for (int qmPosition : qmPositions) {
            if (qmPosition < 0 || qmPosition >= qmTemplate.length()) {
                throw new IllegalArgumentException(String.format("Questionmark postion %s out of range: template=%s, orderedParameters=%s, qmPositions=%s",
                        qmPosition, qmTemplate, orderedParameters, qmPositions));
            }
            if (qmTemplate.charAt(qmPosition) != '?') {
                throw new IllegalArgumentException(
                        String.format("Questionmark expected at position %s in template: template=%s, orderedParameters=%s, qmPositions=%s", qmPosition,
                                qmTemplate, orderedParameters, qmPositions));
            }
        }
        this.qmTemplate = qmTemplate;
        this.orderedParameters = Collections.unmodifiableList(new ArrayList<>(orderedParameters));
        this.qmPositions = Collections.unmodifiableList(new ArrayList<>(qmPositions));

    }

    /**
     * @param values
     * @return true if the list has null-elements
     */
    private static boolean containsAnyNull(List<?> values) {
        return (values.stream().anyMatch(Objects::isNull));
    }

    /**
     * Convenience method to create a {@link PreparedStatement} and apply all parameters
     * 
     * @param conn
     * @return {@link PreparedStatement} with all parameters from this template applied, ready to be executed
     * @throws SQLException
     */
    public PreparedStatement createPreparedStatement(Connection conn) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(this.qmTemplate());
        this.apply(stmt);
        return stmt;
    }

    /**
     * Applies the parameters to the {@link PreparedStatement} created from the {@link #qmTemplate()}.
     * 
     * @param stmt
     * @throws SQLException
     */
    public void apply(PreparedStatement stmt) throws SQLException {
        int idx = 1;
        for (QueryParameter parameter : orderedParameters) {
            if (parameter.value() == null) {
                LOGGER.warn(
                        "Setting a parameter to NULL usually indicates a mistake (write IS [NOT] NULL instead), given: stmt={}, parameterIdx={}, parameter={}",
                        stmt, idx, parameter);
            }
            parameter.adlSqlType().getQueryParameterApplicator().apply(stmt, parameter, idx);
            idx++;
        }
    }

    /**
     * This method returns the SQL-string with the positional parameters replaced with string arguments. It relies on the formatters configured for the
     * {@link AdlSqlType}s.
     * <p>
     * <p>
     * <b>Warning!</b> This method is in general NOT SUITABLE FOR EXECUTING SQLs because (even with escaping!) we cannot harden this against SQL-injection. It
     * is impossible to predict the behavior of all possible driver/db combinations. Only {@link PreparedStatement}s can guarantee sufficient safety.
     * 
     * @see QueryParameterApplicator#applyUnsafe(StringBuilder, QueryParameter, int)
     * 
     * @throws AdlFormattingException in case of errors
     */
    public void applyUnsafe(StringBuilder sb) {
        if (orderedParameters.isEmpty()) {
            sb.append(qmTemplate);
            return;
        }
        int parameterIdx = 0;
        int qmPosition = qmPositions.get(parameterIdx);
        for (int idx = 0; idx < qmTemplate.length(); idx++) {
            if (idx == qmPosition) {
                QueryParameter parameter = orderedParameters.get(parameterIdx);
                if (parameter.value() == null) {
                    LOGGER.warn(
                            "Setting a parameter to NULL usually indicates a mistake (write IS [NOT] NULL instead), given: stmt={}, parameterIdx={}, parameter={}",
                            sb, parameterIdx + 1, parameter);
                }
                parameter.adlSqlType().getQueryParameterApplicator().applyUnsafe(sb, parameter, parameterIdx + 1);
                parameterIdx++;
                if (parameterIdx < qmPositions.size()) {
                    qmPosition = qmPositions.get(parameterIdx);
                }
            }
            else {
                sb.append(qmTemplate.charAt(idx));
            }
        }
    }

    /**
     * This is a fail-safe implementation to ensure debug strings can be composed even if there are formatter issues. <br>
     * Also, warnings will be suppressed.
     * 
     * @param sb to append the template with replaced parameters
     */
    private void applyUnsafeDebug(StringBuilder sb) {
        if (orderedParameters.isEmpty()) {
            sb.append(qmTemplate);
            return;
        }
        int parameterIdx = 0;
        int qmPosition = qmPositions.get(parameterIdx);
        for (int idx = 0; idx < qmTemplate.length(); idx++) {
            if (idx == qmPosition) {
                QueryParameter parameter = orderedParameters.get(parameterIdx);
                try {
                    parameter.adlSqlType().getQueryParameterApplicator().applyUnsafe(sb, parameter, parameterIdx + 1);
                }
                catch (RuntimeException ex) {
                    LOGGER.error("Unable to append parameter value, given: parameter={}, stmt={}", parameter, sb, ex);
                    sb.append("<!ERR:");
                    sb.append(parameter.id());
                    sb.append("!>");
                }
                parameterIdx++;
                if (parameterIdx < qmPositions.size()) {
                    qmPosition = qmPositions.get(parameterIdx);
                }
            }
            else {
                sb.append(qmTemplate.charAt(idx));
            }
        }
    }

    /**
     * This method is fail-safe and falls back to the regular toString()-method if anything goes wrong.<br>
     * In this case the returned String starts with an error indicator.
     * 
     * @return returns a (usually functional) version of the statement, for debugging/logging purposes
     */
    public String toDebugString() {

        StringBuilder sb = new StringBuilder();
        try {
            applyUnsafeDebug(sb);
        }
        catch (RuntimeException ex) {
            LOGGER.error("Unable to create debug string.", ex);
            return "<!ERR!>" + this.toString();
        }
        return sb.toString().trim();
    }

    /**
     * Supplementary class that helps to turn a template with named parameters into meta data for a {@link PreparedStatement} with positional parameters.
     */
    private static class Builder implements ParameterListener {
        private final Map<String, QueryParameter> parameterMap;

        private final StringBuilder sbQuery = new StringBuilder();

        private final List<QueryParameter> orderedParameters = new ArrayList<>();

        private final List<Integer> qmPositions = new ArrayList<>();

        /**
         * relative position for copying pending template content (before, between, and after parameter references)
         */
        private int lastPosition = 0;

        /**
         * Initializes the builder
         * 
         * @param queryTemplate
         * @param parameters
         */
        private Builder(String queryTemplate, List<QueryParameter> parameters) {
            this.parameterMap = createParameterMap(parameters);
            new QueryTemplateParser(this).parseSqlTemplate(queryTemplate);
            if (queryTemplate.length() > lastPosition) {
                sbQuery.append(queryTemplate.substring(lastPosition, queryTemplate.length()));
            }
        }

        /**
         * @return the new prepared instance with aligned set of positional parameters
         */
        private QueryTemplateWithParameters get() {
            return new QueryTemplateWithParameters(sbQuery.toString(), orderedParameters, qmPositions);
        }

        @Override
        public void handleParameter(String id, String template, int fromIdx, int toIdx) {
            QueryParameter parameter = parameterMap.get(id);
            if (parameter == null) {
                throw new QueryPreparationException(String.format("Unknown parameter: id=%s referenced at position=%s in template=%s", id, fromIdx, template));
            }

            orderedParameters.add(parameter);
            if (fromIdx > lastPosition) {
                sbQuery.append(template.substring(lastPosition, fromIdx));
            }
            qmPositions.add(sbQuery.length());
            sbQuery.append('?');
            lastPosition = toIdx;
        }

        /**
         * @param parameters
         * @return each parameter mapped to its unique id
         */
        private static Map<String, QueryParameter> createParameterMap(List<QueryParameter> parameters) {
            if (parameters == null) {
                throw new IllegalArgumentException("List of parameters must not be null.");
            }
            Map<String, QueryParameter> res = new HashMap<>();
            for (QueryParameter parameter : parameters) {
                if (parameter == null) {
                    throw new IllegalArgumentException(String.format("Null element detected, given: %s", parameters));
                }
                QueryParameter prevParam = res.putIfAbsent(parameter.id(), parameter);
                if (prevParam != null && !prevParam.equals(parameter)) {
                    throw new IllegalArgumentException(String.format("Duplicate parameter id=%s detected, given: %s", parameter.id(), parameters));
                }
            }
            return Collections.unmodifiableMap(res);
        }

    }

}
