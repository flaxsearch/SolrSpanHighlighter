package com.github.flaxsearch.solr.spanhighlighter;

import org.junit.*;
import static org.mockito.Mockito.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.assertj.core.api.Assertions;

public class testDocumentHighlighter {

    @Test
    public void testHighlight() {
        FieldType ft = mock(FieldType.class);
        when(ft.getIndexAnalyzer()).thenReturn(new StandardAnalyzer());
        
        IndexSchema schema = mock(IndexSchema.class);
        when(schema.getFieldTypeByName("text")).thenReturn(ft);
        
        Query query = new TermQuery(new Term("text", "banana"));
        
        Query rewritten = QueryRewriter.INSTANCE.rewrite(query);        
        List<HighlightingTask> tasks = Arrays.asList(new HighlightingTask(0, rewritten, "[", "]")); 

        String[] hlFields = new String[] { "text" };
        
        Document doc = makeDoc("text", "what is my banana doing over there?");
        DocumentHighlighter highlighter = new DocumentHighlighter(schema, tasks, hlFields);
        highlighter.highlightDoc(doc);
    }
    
    public static Document makeDoc(String... fields) {
        assert fields.length % 2 == 0;
        Document document = new Document();
        for (int i = 0; i < fields.length; i += 2) {
            String name = fields[i];
            String value = fields[i + 1];            
            document.add(new TextField(name, value, Field.Store.YES));
        }
        return document;
    }

}
