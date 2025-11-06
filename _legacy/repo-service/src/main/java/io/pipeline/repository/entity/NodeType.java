package io.pipeline.repository.entity;

import io.quarkus.hibernate.orm.panache.PanacheEntityBase;
import jakarta.persistence.*;

/**
 * Node type lookup table for different document types.
 */
@Entity
@Table(name = "node_type")
public class NodeType extends PanacheEntityBase {
    
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    public Long id;
    
    @Column(unique = true, nullable = false)
    public String code;                    // Type code (PIPEDOC, PIPESTREAM, etc.)
    
    @Column(nullable = false)
    public String description;             // Human-readable description
    
    @Column(name = "protobuf_type")
    public Boolean protobufType = false;   // Whether this is a protobuf type
    
    // Constructors
    public NodeType() {}
    
    public NodeType(String code, String description, Boolean protobufType) {
        this.code = code;
        this.description = description;
        this.protobufType = protobufType;
    }
    
    // Helper methods
    public static NodeType findByCode(String code) {
        return find("code", code).firstResult();
    }
    
    public static java.util.List<NodeType> findProtobufTypes() {
        return find("protobufType", true).list();
    }
}
