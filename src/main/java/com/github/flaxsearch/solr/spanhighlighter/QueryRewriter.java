package com.github.flaxsearch.solr.spanhighlighter;

/*
 *   Copyright (c) 2017 Lemur Consulting Ltd.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

import java.io.IOException;
import java.util.*;

import org.apache.lucene.index.*;
import org.apache.lucene.search.*;
import org.apache.lucene.search.spans.*;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;



/**
 *   FIXME
 *    
 *   1. All non-span queries are converted to span queries, so that offsets can be collected.
 *   FIXME 2. All fields are renamed "field", so that any query will match in any text field.
 *   FIXME 3. All boolean relationships are converted to OR. This is because we're generating the highlights field-by-field, and need to avoid
 *      the case where an AND query only matches over multiple fields.
 *
 */
public class QueryRewriter {

    private static final Logger LOG = LoggerFactory.getLogger(QueryRewriter.class);

    public static final QueryRewriter INSTANCE = new QueryRewriter();

    public Query rewrite(Query in) {
        if (in instanceof SpanNearQuery)
            return new SpanOffsetReportingQuery((SpanQuery) in);

        if (in instanceof SpanOrQuery)
            return new SpanOffsetReportingQuery((SpanQuery) in);

        if (in instanceof SpanTermQuery)
            return new SpanOffsetReportingQuery((SpanQuery) in);

        if (in instanceof TermQuery)
            return new SpanOffsetReportingQuery(new SpanTermQuery(((TermQuery) in).getTerm()));
        
        if (in instanceof BooleanQuery)
            return rewriteBoolean((BooleanQuery) in);

        if (in instanceof MultiTermQuery)
            return new SpanOffsetReportingQuery(new SpanMultiTermQueryWrapper<>((MultiTermQuery) in));

        if (in instanceof DisjunctionMaxQuery)
            return rewriteDisjunctionMaxQuery((DisjunctionMaxQuery) in);
        
        if (in instanceof TermInSetQuery)
            return rewriteTermsQuery((TermInSetQuery) in);
        
        if (in instanceof SynonymQuery)
            return rewriteSynonymQuery((SynonymQuery) in);
        
        if (in instanceof MultiPhraseQuery)
            return rewriteMultiPhraseQuery((MultiPhraseQuery) in);
        
        if (in instanceof BoostQuery)
            return rewrite(((BoostQuery) in).getQuery());   // we don't care about boosts for rewriting purposes
        
        if (in instanceof PhraseQuery)
            return rewritePhraseQuery((PhraseQuery) in);
        
        LOG.warn("I don't know how to rewrite {}", in);
        return in;
    }
        
    protected Query rewriteDisjunctionMaxQuery(DisjunctionMaxQuery disjunctionMaxQuery) {
        ArrayList<Query> subQueries = new ArrayList<>();
        for (Query subQuery : disjunctionMaxQuery) {
            subQueries.add(rewrite(subQuery));
        }        
        return new DisjunctionMaxQuery(subQueries, disjunctionMaxQuery.getTieBreakerMultiplier());
    }

    protected Query rewriteBoolean(BooleanQuery bq) {
        BooleanQuery.Builder newbq = new BooleanQuery.Builder();
        for (BooleanClause clause : bq) {
            newbq.add(rewrite(clause.getQuery()), clause.getOccur());
        }
        return new ForceNoBulkScoringQuery(newbq.build());
    }

    protected Query rewriteTermsQuery(TermInSetQuery query) {
        List<SpanTermQuery> spanQueries = new ArrayList<>();
        try {
            PrefixCodedTerms terms = query.getTermData();
            PrefixCodedTerms.TermIterator it = terms.iterator();
            for (int i = 0; i < terms.size(); i++) {
                BytesRef bytes = BytesRef.deepCopyOf(it.next());
                spanQueries.add(new SpanTermQuery(new Term(it.field(), bytes)));
            }

            return new SpanOrQuery(spanQueries.toArray(new SpanTermQuery[spanQueries.size()]));
        } 
        catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    protected SpanQuery rewriteSynonymQuery(SynonymQuery query) {
        List<Term> terms = query.getTerms();
        SpanQuery[] qs = new SpanQuery[terms.size()];
        for (int i = 0; i < terms.size(); i++) {
            qs[i] = new SpanTermQuery(terms.get(i));
        }
        return new SpanOrQuery(qs);
    }

    /*
    * This method is only able to rewrite standard phrases where each word must follow the previous one
    * with no gaps or overlaps.  This means, for example, that phrases must not have stopwords in them.
    */
    protected Query rewritePhraseQuery(PhraseQuery query) {
        Term[] terms = query.getTerms();
        int[] positions = query.getPositions();
        SpanTermQuery[] spanQueries = new SpanTermQuery[positions.length];

        for(int i = 0; i < positions.length; i++) {
            if(positions[i] - positions[0] != i) {
                // positions must increase by 1 each time (i-1 is safe as the if can't be true for i=0)
                throw new IllegalArgumentException("Don't know how to rewrite PhraseQuery with holes or overlaps " +
                        "(position must increase by 1 each time but found term " + terms[i-1] + " at position " +
                        positions[i-1] + " followed by term " + terms[i] + " at position " + positions[i] + ")" + query +
                        " (this could be caused, for example, by a stopwords filter in the query analyzer)");
            }

            spanQueries[i] = new SpanTermQuery(terms[i]);
        }

        return new SpanNearQuery(spanQueries, query.getSlop(), true);
    }

    
    protected SpanQuery rewriteMultiPhraseQuery(MultiPhraseQuery query) {
        Term terms[][] = query.getTermArrays();
        int positions[] = query.getPositions();
        String field = terms[0][0].field();
        SpanNearQuery.Builder snq = SpanNearQuery.newOrderedNearQuery(field);
        for (int i = 0; i < terms.length; i++) {
            if (i > 1 && positions[i] - positions[i - 1] > 0) {
                snq.addGap(positions[i] - positions[i - 1]);
            }
            if (terms[i].length == 1) {
                snq.addClause(new SpanTermQuery(terms[i][0]));
            }
            else {
                SpanQuery ors[] = new SpanQuery[terms[i].length];
                for (int j = 0; j < ors.length; j++) {
                    ors[j] = new SpanTermQuery(terms[i][j]);
                }
                snq.addClause(new SpanOrQuery(ors));
            }
        }
        return snq.build();
    }
    
    /**
     * SpanQuery that wraps another SpanQuery, ensuring that offsets are loaded
     * from the postings lists and exposed to SpanCollectors.
     */
    private static class SpanOffsetReportingQuery extends SpanQuery {

        private final SpanQuery in;

        /**
         * Create a new SpanOffsetReportingQuery
         * @param in the query to wrap
         */
        public SpanOffsetReportingQuery(SpanQuery in) {
            this.in = in;
        }

        @Override
        public String getField() {
            return in.getField();
        }

        @Override
        public String toString(String field) {
            return in.toString();
        }

        @Override
        public Query rewrite(IndexReader reader) throws IOException {
            SpanQuery rewritten = (SpanQuery) in.rewrite(reader);
            if (in == rewritten)
                return this;
            return new SpanOffsetReportingQuery((SpanQuery)in.rewrite(reader));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            SpanOffsetReportingQuery that = (SpanOffsetReportingQuery) o;
            return Objects.equals(in, that.in);
        }

        @Override
        public int hashCode() {
            return Objects.hash(in);
        }

        @Override
        public SpanWeight createWeight(IndexSearcher searcher, boolean needsScores, float boost) throws IOException {
            return new SpanOffsetWeight(searcher, in.createWeight(searcher, needsScores, boost), boost);
        }

        /**
         * Build a map of terms to termcontexts, for use in constructing SpanWeights
         */
        private static Map<Term, TermContext> termContexts(SpanWeight... weights) {
            Map<Term, TermContext> terms = new TreeMap<>();
            for (SpanWeight w : weights) {
                w.extractTermContexts(terms);
            }
            return terms;
        }
    
        private class SpanOffsetWeight extends SpanWeight {
    
            private final SpanWeight in;
    
            private SpanOffsetWeight(IndexSearcher searcher, SpanWeight in, float boost) throws IOException {
                super(SpanOffsetReportingQuery.this, searcher, termContexts(in), boost);
                this.in = in;
            }
    
            @Override
            public void extractTermContexts(Map<Term, TermContext> contexts) {
                in.extractTermContexts(contexts);
            }
    
            @Override
            public Spans getSpans(LeafReaderContext ctx, Postings requiredPostings) throws IOException {
                return in.getSpans(ctx, Postings.OFFSETS);
            }
    
            @Override
            public void extractTerms(Set<Term> terms) {
                in.extractTerms(terms);
            }
            
            @Override
            public boolean isCacheable(LeafReaderContext ctx) {
                // FIXME just added to enable build
                return false;
            }
        }
    }
    
    private static class ForceNoBulkScoringQuery extends Query {

        private final Query inner;

        public ForceNoBulkScoringQuery(Query inner) {
            this.inner = inner;
        }

        @Override
        public Query rewrite(IndexReader reader) throws IOException {
            Query rewritten = inner.rewrite(reader);
            if (rewritten != inner)
                return new ForceNoBulkScoringQuery(rewritten);
            return super.rewrite(reader);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ForceNoBulkScoringQuery that = (ForceNoBulkScoringQuery) o;
            return Objects.equals(inner, that.inner);
        }

        @Override
        public int hashCode() {
            return Objects.hash(inner);
        }

        public Query getWrappedQuery() {
            return inner;
        }

        @Override
        public Weight createWeight(IndexSearcher searcher, boolean needsScores, float boost) throws IOException {

            final Weight innerWeight = inner.createWeight(searcher, needsScores, boost);

            return new Weight(ForceNoBulkScoringQuery.this) {
                @Override
                public void extractTerms(Set<Term> set) {
                    innerWeight.extractTerms(set);
                }

                @Override
                public Explanation explain(LeafReaderContext leafReaderContext, int i) throws IOException {
                    return innerWeight.explain(leafReaderContext, i);
                }

                @Override
                public Scorer scorer(LeafReaderContext leafReaderContext) throws IOException {
                    return innerWeight.scorer(leafReaderContext);
                }
                
                @Override
                public boolean isCacheable(LeafReaderContext ctx) {
                    // FIXME just added to enable build
                    return false;
                }
            };
        }

        @Override
        public String toString(String s) {
            return "NoBulkScorer(" + inner.toString(s) + ")";
        }
    }

}
