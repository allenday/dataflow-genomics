package com.google.allenday.genomics.core.model;

import com.google.allenday.genomics.core.utils.StringUtils;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import java.io.Serializable;
import java.util.Objects;

@DefaultCoder(AvroCoder.class)
public class SamRecordsChunkMetadataKey implements Serializable {

    private SraSampleId sraSampleId;
    private String referenceName;
    private Region region;

    public SamRecordsChunkMetadataKey() {
    }

    public SamRecordsChunkMetadataKey(SraSampleId sraSampleId, String referenceName) {
        this.sraSampleId = sraSampleId;
        this.referenceName = referenceName;
    }

    public SamRecordsChunkMetadataKey(SraSampleId sraSampleId, String referenceName, Region region) {
        this(sraSampleId, referenceName);
        this.region = region;
    }

    public SraSampleId getSraSampleId() {
        return sraSampleId;
    }

    public String getReferenceName() {
        return referenceName;
    }

    public Region getRegion() {
        return region;
    }

    public String generateFileSuffix() {
        StringBuilder suffixBuilder = new StringBuilder();
        suffixBuilder.append("_").append(StringUtils.generateSlug(referenceName));
        if (!region.equals(Region.UNDEFINED)) {
            if (!region.isMapped()) {
                suffixBuilder
                        .append("_").append("not_mapped");
            } else {
                suffixBuilder
                        .append("_").append(StringUtils.generateSlug(region.contig));
            }
            suffixBuilder.append("_").append(region.start)
                    .append("_").append(region.end);
        }
        return suffixBuilder.toString();
    }

    public String generateSlug() {
        return sraSampleId.getValue() + generateFileSuffix();
    }

    public String generatRegionsValue() {
        return region != null ? region.contig + ":" + region.start + "-" + region.end : null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SamRecordsChunkMetadataKey that = (SamRecordsChunkMetadataKey) o;
        return Objects.equals(sraSampleId, that.sraSampleId) &&
                Objects.equals(referenceName, that.referenceName) &&
                Objects.equals(region, that.region);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sraSampleId, referenceName, region);
    }

    @Override
    public String toString() {
        return "SamRecordsChunkMetadataKey{" +
                "sraSampleId=" + sraSampleId +
                ", referenceName='" + referenceName + '\'' +
                ", region=" + region +
                '}';
    }

    public SamRecordsChunkMetadataKey cloneWithUndefinedRegion() {
        return new SamRecordsChunkMetadataKey(SraSampleId.create(sraSampleId.getValue()), referenceName, Region.UNDEFINED);
    }

    @DefaultCoder(AvroCoder.class)
    public static class Region implements Serializable {

        public static Region UNDEFINED = new Region("", -1l, -1l);

        private String contig;
        private Long start;
        private Long end;

        public Region() {
        }

        public Region(String contig, Long start, Long end) {
            this.contig = contig;
            this.start = start;
            this.end = end;
        }

        public String getContig() {
            return contig;
        }

        public Long getStart() {
            return start;
        }

        public Long getEnd() {
            return end;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Region region = (Region) o;
            return Objects.equals(contig, region.contig) &&
                    Objects.equals(start, region.start) &&
                    Objects.equals(end, region.end);
        }

        @Override
        public int hashCode() {
            return Objects.hash(contig, start, end);
        }

        @Override
        public String toString() {
            return "Region{" +
                    "contig='" + contig + '\'' +
                    ", start=" + start +
                    ", end=" + end +
                    '}';
        }

        public boolean isMapped() {
            return !contig.equals("*");
        }
    }
}