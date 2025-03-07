import os
import sys
import sqlite3
from py2neo import Graph, Node, Relationship

ORG_ROAM_DIR = os.getenv("ORG_ROAM_DIR")
ORG_ROAM_DB = os.getenv("ORG_ROAM_DB")
NEO4J_URI = os.getenv("NEO4J_URI")
NEO4J_USER = os.getenv("NEO4J_USER")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD")


def extract_org_roam_data():
    """Extract data from Org-roam SQLite database."""
    if not os.path.exists(ORG_ROAM_DB):
        print(f"Error: Database file not found at {ORG_ROAM_DB}")
        return None, None, None
    
    try:
        conn = sqlite3.connect(ORG_ROAM_DB)
        conn.row_factory = sqlite3.Row
        
        # Get nodes (files and their titles)
        nodes_query = """
        SELECT id, file, title, level
        FROM nodes
        """
        
        # Get links between nodes
        links_query = """
        SELECT source, dest, type
        FROM links
        """
        
        # Get tags for nodes
        tags_query = """
        SELECT node_id, tag
        FROM tags
        """
        
        nodes = conn.execute(nodes_query).fetchall()
        links = conn.execute(links_query).fetchall()
        tags = conn.execute(tags_query).fetchall()
        
        conn.close()
        
        print(f"Extracted {len(nodes)} nodes, {len(links)} links, and {len(tags)} tags from SQLite")
        return nodes, links, tags
    except Exception as e:
        print(f"Error extracting data from SQLite: {e}")
        return None, None, None

def load_to_neo4j(nodes, links, tags):
    """Load extracted data into Neo4j."""
    if not nodes:
        print("No data to export to Neo4j")
        return False
    
    try:
        # Connect to Neo4j
        print(f"Connecting to Neo4j at {NEO4J_URI}...")
        graph = Graph(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        
        # Clear existing data
        print("Clearing existing Neo4j data...")
        graph.run("MATCH (n) DETACH DELETE n")
        
        # Create nodes
        print("Creating nodes in Neo4j...")
        for i, node in enumerate(nodes):
            if i % 100 == 0 and i > 0:
                print(f"Created {i}/{len(nodes)} nodes...")
            
            neo4j_node = Node(
                "OrgNode",
                id=node['id'],
                file=node['file'],
                title=node['title'],
                level=node['level']
            )
            graph.create(neo4j_node)
        
        print(f"Successfully created {len(nodes)} nodes in Neo4j")
        
        # Create relationships from links
        print("Creating links in Neo4j...")
        successful_links = 0
        for i, link in enumerate(links):
            if i % 100 == 0 and i > 0:
                print(f"Processed {i}/{len(links)} links...")
            
            query = """
            MATCH (source:OrgNode {id: $source_id})
            MATCH (dest:OrgNode {id: $dest_id})
            CREATE (source)-[r:LINKS_TO {type: $link_type}]->(dest)
            RETURN r
            """
            
            try:
                result = graph.run(query, 
                                  source_id=link['source'], 
                                  dest_id=link['dest'], 
                                  link_type=link['type'])
                if result:
                    successful_links += 1
            except Exception as e:
                # Some links might be to non-existent nodes, which is expected
                pass
        
        print(f"Successfully created {successful_links} links in Neo4j")
        
        # Create tags
        print("Creating tags in Neo4j...")
        successful_tags = 0
        for i, tag_entry in enumerate(tags):
            if i % 100 == 0 and i > 0:
                print(f"Processed {i}/{len(tags)} tags...")
            
            # Create tag node if it doesn't exist
            tag_query = """
            MERGE (t:Tag {name: $tag_name})
            RETURN t
            """
            graph.run(tag_query, tag_name=tag_entry['tag'])
            
            # Link tag to node
            tag_link_query = """
            MATCH (n:OrgNode {id: $node_id})
            MATCH (t:Tag {name: $tag_name})
            CREATE (n)-[r:HAS_TAG]->(t)
            RETURN r
            """
            
            try:
                result = graph.run(tag_link_query, 
                                  node_id=tag_entry['node_id'], 
                                  tag_name=tag_entry['tag'])
                if result:
                    successful_tags += 1
            except Exception as e:
                # Some tags might link to non-existent nodes, which is expected
                pass
        
        print(f"Successfully created {successful_tags} tag relationships in Neo4j")
        
       
        print("Export to Neo4j completed successfully")
        return True
    except Exception as e:
        print(f"Error exporting to Neo4j: {e}")
        return False

def verify_neo4j_data():
    """Verify the data was properly exported to Neo4j."""
    try:
        # Connect to Neo4j
        graph = Graph(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
        
        # Count nodes
        node_count = graph.run("MATCH (n:OrgNode) RETURN count(n) as count").data()[0]['count']
        print(f"Neo4j contains {node_count} OrgNodes")
        
        # Count links
        link_count = graph.run("MATCH (:OrgNode)-[r:LINKS_TO]->(:OrgNode) RETURN count(r) as count").data()[0]['count']
        print(f"Neo4j contains {link_count} LINKS_TO relationships")
        
        # Count tags
        tag_count = graph.run("MATCH (t:Tag) RETURN count(t) as count").data()[0]['count']
        print(f"Neo4j contains {tag_count} Tags")
        
        # Count tag relationships
        tag_rel_count = graph.run("MATCH (:OrgNode)-[r:HAS_TAG]->(:Tag) RETURN count(r) as count").data()[0]['count']
        print(f"Neo4j contains {tag_rel_count} HAS_TAG relationships")
        
        # Nodes with most links
        print("\nTop 5 nodes with most outgoing links:")
        top_linkers = graph.run("""
            MATCH (n:OrgNode)-[r:LINKS_TO]->()
            RETURN n.title as title, count(r) as link_count
            ORDER BY link_count DESC
            LIMIT 5
        """).data()
        
        for node in top_linkers:
            print(f"  - {node['title']}: {node['link_count']} links")
        
        # Nodes with most backlinks
        print("\nTop 5 nodes with most incoming links (backlinks):")
        top_backlinks = graph.run("""
            MATCH (n:OrgNode)<-[r:LINKS_TO]-()
            RETURN n.title as title, count(r) as backlink_count
            ORDER BY backlink_count DESC
            LIMIT 5
        """).data()
        
        for node in top_backlinks:
            print(f"  - {node['title']}: {node['backlink_count']} backlinks")
        
        # Most used tags
        print("\nTop 5 most used tags:")
        top_tags = graph.run("""
            MATCH (t:Tag)<-[r:HAS_TAG]-()
            RETURN t.name as tag, count(r) as usage_count
            ORDER BY usage_count DESC
            LIMIT 5
        """).data()
        
        for tag in top_tags:
            print(f"  - {tag['tag']}: {tag['usage_count']} nodes")
        
        return True
    except Exception as e:
        print(f"Error verifying Neo4j data: {e}")
        return False

def main():
    print("Org-roam to Neo4j Exporter")
    print("==========================")
    
    # Step 1: Extract data from Org-roam SQLite database
    print("\nStep 1: Extracting data from Org-roam database...")
    nodes, links, tags = extract_org_roam_data()
    
    if not nodes:
        print("Failed to extract data from Org-roam database")
        return 1
    
    # Step 2: Load data into Neo4j
    print("\nStep 2: Loading data into Neo4j...")
    if not load_to_neo4j(nodes, links, tags):
        print("Failed to export data to Neo4j")
        return 1
    
    # Step 3: Verify the data in Neo4j
    print("\nStep 3: Verifying data in Neo4j...")
    if not verify_neo4j_data():
        print("Failed to verify data in Neo4j")
        return 1
    
    print("\nExport completed successfully!")
    print("Your Org-roam data is now available in Neo4j")
    
    print("\nUseful Neo4j Cypher queries:")
    print("1. Find all nodes: MATCH (n:OrgNode) RETURN n LIMIT 100")
    print("2. Find backlinks to a specific node: MATCH (n:OrgNode {title: 'Your Title'})<-[:LINKS_TO]-(source) RETURN source")
    print("3. Find nodes by tag: MATCH (n:OrgNode)-[:HAS_TAG]->(:Tag {name: 'your_tag'}) RETURN n")
    print("4. Find orphan nodes (no links): MATCH (n:OrgNode) WHERE NOT (n)-[:LINKS_TO]->() AND NOT ()-[:LINKS_TO]->(n) RETURN n")
    
    return 0

if __name__ == "__main__":
    sys.exit(main())
