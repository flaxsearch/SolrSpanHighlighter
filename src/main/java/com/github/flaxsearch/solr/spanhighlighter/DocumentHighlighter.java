package com.github.flaxsearch.solr.spanhighlighter;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.memory.MemoryIndex;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.SimpleCollector;
import org.apache.lucene.search.spans.SpanCollector;
import org.apache.solr.schema.FieldType;
import org.apache.solr.schema.IndexSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DocumentHighlighter {
    
    private static final Logger LOG = LoggerFactory.getLogger(DocumentHighlighter.class);

    IndexSchema schema;
    List<HighlightingTask> tasks;
    List<Pattern> hlFieldPatterns;
    
    public DocumentHighlighter(IndexSchema schema, List<HighlightingTask> tasks, String[] hlFields) {
        this.schema = schema;
        this.tasks = tasks;
        this.hlFieldPatterns = Arrays.stream(hlFields).map(x -> 
            Pattern.compile(x.replace("*",  ".*"))).collect(Collectors.toList());
    }
    
    public Map<String, List<String>> highlightDoc(Document doc) {
        
        // build a single-field, single-document, memory index
        MemoryIndex index = new MemoryIndex(true);
        for (IndexableField field : doc) {
            if (hlFieldPatterns.stream().anyMatch(x -> x.matcher(field.name()).matches())) {
                FieldType ft = schema.getFieldTypeByName(field.name());
                Analyzer analyzer = ft.getIndexAnalyzer();
                LOG.trace("field name={} value={}... analyzer={}", field.name(), field.stringValue().substring(0,  10), analyzer);
                if (analyzer != null && field.stringValue() != null) {
                    index.addField(field.name(), field.stringValue(), analyzer);
                }
            }
        }        
        index.freeze();

        IndexSearcher searcher = index.createSearcher();
        searcher.setQueryCache(null);
        
        // collect the matching spans for each of the highlighting tasks (in the same sorted collector)
        HlSpanCollector collector = new HlSpanCollector();
        for (HighlightingTask task : tasks) {
            LOG.debug("collecting spans for {}", task);
            collector.setTask(task);
            try {
                searcher.search(task.query, new SimpleCollector() {    
                    Scorer scorer;
    
                    @Override
                    public boolean needsScores() {
                        return true;
                    }
        
                    @Override
                    public void setScorer(Scorer scorer) throws IOException {
                        this.scorer = scorer;
                    }
        
                    @Override
                    public void collect(int doc) throws IOException {
                        SpanExtractor.collect(scorer, collector);
                    }
                });
            }
            catch (IOException e) {
                LOG.error("error collecting spans", e);
            }
        }
        
        for (Offset off : collector.offsets) {
            System.out.println("FIXME offset=" + off);
        }

        return null;
    }

    /**
     * Actually, a pair of offsets, which indicate where the "pre" and "post" highlighting tags should be inserted.
     * Also includes the highlighting task which generated this offset, so we know the priorities for merging.  
     */
    private static class Offset implements Comparable<Offset> {
        public int start;
        public int end;
        public HighlightingTask task;
        
        public Offset(int start, int end, HighlightingTask task) {
            this.start = start;
            this.end = end;
            this.task = task;
        }
        
        @Override
        public int compareTo(Offset that) {
            return Integer.compare(this.start, that.start);
        }
        
        @Override
        public String toString() {
            return "Offset(" + start + ", " + end + ")";
        }
        
        /**
         * @return true, iff the supplied offset overlaps with this one.
         */
        public boolean overlaps(Offset that) {
            return this.end >= that.start && this.start <= that.end;
        }
        
        /**
         * @return a merged offset of this and the overlapping one
         */
        public Offset merge(Offset that) {
            HighlightingTask priorityTask = (this.task.priority < that.task.priority) ? this.task : that.task;
            return new Offset(Math.min(this.start, that.start), Math.max(this.end, that.end), priorityTask);
        }
    }

    /**
     * Convenience class for collecting offsets from a span query.
     */
    private static class HlSpanCollector implements SpanCollector {
        SortedSet<Offset> offsets = new TreeSet<>();
        private HighlightingTask currentTask;
        
        public void setTask(HighlightingTask task) {
            currentTask = task;
        }
        
        @Override
        public void collectLeaf(PostingsEnum postingsEnum, int i, Term term) throws IOException {
            offsets.add(new Offset(postingsEnum.startOffset(), postingsEnum.endOffset(), currentTask));
        }
        
        @Override
        public void reset() { }        
    }    
}
