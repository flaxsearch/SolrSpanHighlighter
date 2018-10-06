package com.github.flaxsearch.solr.spanhighlighter;

import org.apache.lucene.search.Query;

/**
 * This class bundles a query to highlight, tags to use to highlight terms in the text, and a priority (lower values "win").
 * The purpose of this class is to simplify the code when the user specifies multiple highlighting, although it is also used
 * for single-query highlighting.
 */
public class HighlightingTask {
    int priority;
    Query query;
    String startTag;
    String endTag;        
    
    public HighlightingTask(int priority, Query query, String startTag, String endTag) {
        this.priority = priority;
        this.query = query;
        this.startTag = startTag;
        this.endTag = endTag;
    }
    
    public String toString() {
        return String.format("HighlightingTask(%d, %s, %s, %s)", priority, query, startTag, endTag);
    }
}
