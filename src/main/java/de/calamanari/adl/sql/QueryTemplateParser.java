//@formatter:off
/*
 * QueryTemplateParser
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

/**
 * A {@link QueryParameter} parses a template with named parameters (<code>${id}</code>) to trigger the registered listener on any occurrence with the name and
 * the exact position.
 * 
 * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
 */
public class QueryTemplateParser {

    private final ParameterListener listener;

    /**
     * Creates a new instance with the given {@link ParameterListener} to be triggered
     * 
     * @param listener
     */
    public QueryTemplateParser(ParameterListener listener) {
        if (listener == null) {
            throw new IllegalArgumentException("Listener must not be null.");
        }
        this.listener = listener;
    }

    /**
     * Parses the given template and informs the listener about any parameter occurrence.
     * <p>
     * This allows the listener to conveniently convert the template in a final form (replace parameters).
     * 
     * @param template
     */
    public void parseSqlTemplate(String template) {
        if (template == null) {
            throw new IllegalArgumentException("Template must not be null.");
        }

        int idx = 0;
        while (idx < template.length()) {
            char ch = template.charAt(idx);
            if (ch == '$' && idx < template.length() - 1 && template.charAt(idx + 1) == '{') {
                idx = processCandidate(template, idx);
                if (idx < 0) {
                    throw new QueryPreparationException(String.format(
                            "Error processing template %s, syntax error at position %s (parameter reference not closed, expected ${id}).", template, idx));
                }
            }
            else {
                idx++;
            }
        }

    }

    /**
     * Called after detecting the prefix '<code>${</code>' of a named parameter. Checks for the proper termination of the reference and calls the listener.
     * 
     * @param template
     * @param fromIdx start index of the reference in the template
     * @return position right after the reference or -1 to indicate a problem (reference not properly closed)
     */
    private int processCandidate(String template, int fromIdx) {
        if (fromIdx + 2 >= template.length()) {
            return -1;
        }
        for (int idx = fromIdx + 2; idx < template.length(); idx++) {
            char ch = template.charAt(idx);
            if (ch == '}') {
                String id = template.substring(fromIdx + 2, idx).trim();
                if (id.isEmpty()) {
                    throw new QueryPreparationException(
                            String.format("Error processing template %s, syntax error at position %s (expected ${id}).", template, fromIdx));
                }
                listener.handleParameter(id, template, fromIdx, idx + 1);
                return idx + 1;
            }
        }
        return -1;
    }

    /**
     * When parsing a template a listener can replace the parameter references, typically with question marks and collect the parameters in correct order.
     * 
     * @author <a href="mailto:Karl.Eilebrecht(a/t)calamanari.de">Karl Eilebrecht</a>
     */
    public interface ParameterListener {

        /**
         * Informs the listener about a parameter reference found in the template.
         * 
         * @param id identifies the parameter
         * @param template the currently parsed template
         * @param fromIdx start position in the template (inclusive)
         * @param toIdx end position of the reference in the template (exclusive)
         */
        void handleParameter(String id, String template, int fromIdx, int toIdx);

    }
}
