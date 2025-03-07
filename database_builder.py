#!/usr/bin/env python3
import os
import sys
import sqlite3
import glob
import re
import hashlib

# Configuration
ORG_ROAM_DIR = os.getenv("ORG_ROAM_DIR")  # Your org-roam directory
ORG_ROAM_DB = os.getenv("ORG_ROAM_DB")

def clear_and_create_db():
    """Clear the database if it exists and create a fresh schema."""
    print(f"Clearing and recreating database at {ORG_ROAM_DB}...")
    
    # Remove existing database file if it exists
    if os.path.exists(ORG_ROAM_DB):
        try:
            os.remove(ORG_ROAM_DB)
            print(f"Removed existing database file: {ORG_ROAM_DB}")
        except Exception as e:
            print(f"Error removing existing database: {e}")
            return False
    
    try:
        # Connect to SQLite database (will create a new one)
        conn = sqlite3.connect(ORG_ROAM_DB)
        cursor = conn.cursor()
        
        # Create the 'nodes' table
        cursor.execute('''
        CREATE TABLE nodes (
            id TEXT PRIMARY KEY,
            file TEXT NOT NULL,
            title TEXT,
            level INTEGER DEFAULT 0
        )
        ''')
        
        # Create the 'links' table
        cursor.execute('''
        CREATE TABLE links (
            source TEXT NOT NULL,
            dest TEXT NOT NULL,
            type TEXT DEFAULT 'id',
            FOREIGN KEY(source) REFERENCES nodes(id),
            FOREIGN KEY(dest) REFERENCES nodes(id)
        )
        ''')
        
        # Create the 'tags' table
        cursor.execute('''
        CREATE TABLE tags (
            node_id TEXT NOT NULL,
            tag TEXT NOT NULL,
            FOREIGN KEY(node_id) REFERENCES nodes(id)
        )
        ''')
        
        # Create additional indices for performance
        cursor.execute('CREATE INDEX nodes_file_idx ON nodes(file)')
        cursor.execute('CREATE INDEX links_source_idx ON links(source)')
        cursor.execute('CREATE INDEX links_dest_idx ON links(dest)')
        cursor.execute('CREATE INDEX tags_node_id_idx ON tags(node_id)')
        
        conn.commit()
        conn.close()
        
        print("Fresh database schema created successfully")
        return True
    except Exception as e:
        print(f"Error creating database schema: {e}")
        return False

def extract_id_from_filename(org_file, base_dir):
    """Extract ID from filename following Org-roam conventions."""
    rel_path = os.path.relpath(org_file, base_dir)
    
    # Remove .org extension if present
    if rel_path.endswith('.org'):
        rel_path = rel_path[:-4]
    
    # Check for ID property in file
    try:
        with open(org_file, 'r', encoding='utf-8') as f:
            content = f.read(5000)  # Read beginning of file
            id_match = re.search(r':ID:\s*([a-zA-Z0-9_-]+)', content)
            if id_match:
                return id_match.group(1).strip()
    except:
        pass
    
    # Generate a consistent ID from filename
    hash_obj = hashlib.sha1(rel_path.encode('utf-8'))
    return hash_obj.hexdigest()[:7]  # Use first 7 chars like git

def extract_links_from_content(content):
    """Extract all types of Org-roam links from content."""
    links = []
    
    # Regular [[file:path/to/file.org]] or [[file:path/to/file.org][Description]] links
    file_links = re.findall(r'\[\[file:([^]]+)(?:\]\[([^]]+))?\]\]', content)
    for link, _ in file_links:
        # Clean up the link (remove .org extension if present)
        if link.endswith('.org'):
            link = link[:-4]
        links.append(link)
    
    # ID links like [[id:20210101T123456]]
    id_links = re.findall(r'\[\[id:([^]]+)(?:\]\[([^]]+))?\]\]', content)
    for link_id, _ in id_links:
        links.append(link_id)
    
    # Regular [[text]] wiki-style links
    wiki_links = re.findall(r'\[\[([^]:/]+)(?:\]\[([^]]+))?\]\]', content)
    for link, _ in wiki_links:
        links.append(link)
    
    return links

def extract_tags_from_content(content):
    """Extract all types of tags from content."""
    tags = set()
    
    # Extract #+filetags: tag1:tag2:tag3
    filetag_matches = re.findall(r'#\+filetags:\s*(.*)', content, re.IGNORECASE)
    for tag_line in filetag_matches:
        for tag in re.findall(r':([a-zA-Z0-9_-]+):', tag_line):
            tags.add(tag)
    
    # Extract :PROPERTIES: block tags
    # Look for :TAGS: tag1 tag2 tag3
    tag_prop_matches = re.findall(r':TAGS:\s*(.*)', content)
    for tag_line in tag_prop_matches:
        for tag in tag_line.split():
            # Remove any colons
            clean_tag = tag.strip(':')
            if clean_tag:
                tags.add(clean_tag)
    
    # Extract inline tags like :tag:
    # This is simplified and might pick up some false positives
    inline_tags = re.findall(r':([\w-]+):', content)
    for tag in inline_tags:
        tags.add(tag)
    
    return list(tags)

def scan_and_populate_db():
    """Scan .org files and populate the database."""
    print(f"Scanning for .org files in {ORG_ROAM_DIR}...")
    
    # Find all .org files recursively
    org_files = glob.glob(os.path.join(ORG_ROAM_DIR, "**/*.org"), recursive=True)
    
    if not org_files:
        print(f"No .org files found in {ORG_ROAM_DIR}")
        return False
    
    print(f"Found {len(org_files)} .org files")
    
    try:
        # Connect to the database
        conn = sqlite3.connect(ORG_ROAM_DB)
        cursor = conn.cursor()
        
        # First pass: create all nodes
        node_ids = {}  # Map from filename to node ID
        
        print("First pass: Creating nodes...")
        for i, org_file in enumerate(org_files):
            if i % 10 == 0:
                print(f"Processing node {i+1}/{len(org_files)}...")
            
            # Generate ID for the file
            file_id = extract_id_from_filename(org_file, ORG_ROAM_DIR)
            rel_path = os.path.relpath(org_file, ORG_ROAM_DIR)
            node_ids[rel_path] = file_id
            
            # Extract title and content
            title = os.path.splitext(os.path.basename(org_file))[0]  # Default title
            content = ""
            
            try:
                with open(org_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                    
                    # Look for #+title: tag
                    title_match = re.search(r'#\+title:\s*(.*)', content, re.IGNORECASE)
                    if title_match:
                        title = title_match.group(1).strip()
                    else:
                        # Look for first headline
                        headline_match = re.search(r'^\*\s*(.*)', content, re.MULTILINE)
                        if headline_match:
                            title = headline_match.group(1).strip()
            except Exception as e:
                print(f"Warning: Could not read content from {org_file}: {e}")
            
            # Add node to database
            cursor.execute(
                "INSERT OR REPLACE INTO nodes (id, file, title, level) VALUES (?, ?, ?, ?)",
                (file_id, rel_path, title, 0)
            )
            
            # Process tags
            tags = extract_tags_from_content(content)
            for tag in tags:
                cursor.execute(
                    "INSERT OR REPLACE INTO tags (node_id, tag) VALUES (?, ?)",
                    (file_id, tag)
                )
        
        conn.commit()
        
        # Second pass: create links
        print("\nSecond pass: Creating links...")
        for i, org_file in enumerate(org_files):
            if i % 10 == 0:
                print(f"Processing links {i+1}/{len(org_files)}...")
            
            rel_path = os.path.relpath(org_file, ORG_ROAM_DIR)
            source_id = node_ids.get(rel_path)
            
            if not source_id:
                continue
            
            try:
                with open(org_file, 'r', encoding='utf-8') as f:
                    content = f.read()
                    
                    # Extract links
                    links = extract_links_from_content(content)
                    
                    for link in links:
                        # Try to find target by ID first
                        cursor.execute("SELECT id FROM nodes WHERE id = ?", (link,))
                        result = cursor.fetchone()
                        
                        if result:
                            # Direct ID match
                            dest_id = result[0]
                        else:
                            # Try to match by filename
                            link_path = link
                            if not link_path.endswith('.org'):
                                link_path += '.org'
                            
                            cursor.execute("SELECT id FROM nodes WHERE file LIKE ?", ('%' + link_path,))
                            result = cursor.fetchone()
                            
                            if result:
                                dest_id = result[0]
                            else:
                                # No matching node found, create a placeholder
                                dest_id = extract_id_from_filename(link, "")
                                cursor.execute(
                                    "INSERT OR IGNORE INTO nodes (id, file, title, level) VALUES (?, ?, ?, ?)",
                                    (dest_id, link, link, 0)
                                )
                        
                        # Add link to database
                        cursor.execute(
                            "INSERT OR REPLACE INTO links (source, dest, type) VALUES (?, ?, ?)",
                            (source_id, dest_id, 'id')
                        )
            except Exception as e:
                print(f"Warning: Error processing links in {org_file}: {e}")
        
        conn.commit()
        
        # Get count of nodes, links, and tags
        cursor.execute("SELECT COUNT(*) FROM nodes")
        node_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM links")
        link_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM tags")
        tag_count = cursor.fetchone()[0]
        
        conn.close()
        
        print(f"Database populated with {node_count} nodes, {link_count} links, and {tag_count} tags")
        return True
    except Exception as e:
        print(f"Error populating database: {e}")
        return False

def print_database_stats():
    """Print some statistics about the database."""
    try:
        conn = sqlite3.connect(ORG_ROAM_DB)
        cursor = conn.cursor()
        
        # Count nodes
        cursor.execute("SELECT COUNT(*) FROM nodes")
        node_count = cursor.fetchone()[0]
        print(f"Total nodes: {node_count}")
        
        # Sample of nodes
        cursor.execute("SELECT id, file, title FROM nodes LIMIT 5")
        nodes = cursor.fetchall()
        print("Sample nodes:")
        for node in nodes:
            print(f"  - ID: {node[0]}, File: {node[1]}, Title: {node[2]}")
        
        # Count links
        cursor.execute("SELECT COUNT(*) FROM links")
        link_count = cursor.fetchone()[0]
        print(f"Total links: {link_count}")
        
        # Nodes with most links
        cursor.execute("""
            SELECT n.id, n.title, COUNT(l.source) AS link_count
            FROM nodes n
            JOIN links l ON n.id = l.source
            GROUP BY n.id
            ORDER BY link_count DESC
            LIMIT 5
        """)
        top_linkers = cursor.fetchall()
        print("Top nodes with outgoing links:")
        for node in top_linkers:
            print(f"  - {node[1]} (ID: {node[0]}): {node[2]} links")
        
        # Nodes with most backlinks
        cursor.execute("""
            SELECT n.id, n.title, COUNT(l.dest) AS backlink_count
            FROM nodes n
            JOIN links l ON n.id = l.dest
            GROUP BY n.id
            ORDER BY backlink_count DESC
            LIMIT 5
        """)
        top_backlinks = cursor.fetchall()
        print("Top nodes with incoming links (backlinks):")
        for node in top_backlinks:
            print(f"  - {node[1]} (ID: {node[0]}): {node[2]} backlinks")
        
        # Count tags
        cursor.execute("SELECT COUNT(DISTINCT tag) FROM tags")
        tag_count = cursor.fetchone()[0]
        print(f"Total unique tags: {tag_count}")
        
        # Most used tags
        cursor.execute("""
            SELECT tag, COUNT(*) AS tag_count
            FROM tags
            GROUP BY tag
            ORDER BY tag_count DESC
            LIMIT 5
        """)
        top_tags = cursor.fetchall()
        print("Most used tags:")
        for tag in top_tags:
            print(f"  - {tag[0]}: {tag[1]} nodes")
        
        conn.close()
        return True
    except Exception as e:
        print(f"Error getting database stats: {e}")
        return False

def main():
    print("Org-roam Database Rebuilder")
    print("===========================")
    
    # Step 1: Clear and create database
    if not clear_and_create_db():
        print("Failed to create database schema")
        return 1
    
    # Step 2: Scan .org files and populate database
    if not scan_and_populate_db():
        print("Failed to populate database")
        return 1
    
    # Step 3: Print stats about the database
    print("\nDatabase statistics:")
    print_database_stats()
    
    print("\nDatabase built successfully!")
    print(f"Database file: {ORG_ROAM_DB}")
    print("You can now use your Neo4j export script to transfer this data to Neo4j.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
