#!/usr/bin/env python3
"""
Data Cleaning Pipeline for AASLD Guidelines JSON Files
Prepares data for RAG indexing by:
- Removing navigation/boilerplate content
- Normalizing text formatting
- Extracting recommendations, dosages, and clinical metadata
- Preserving structure (sections, tables, lists)
- Preparing chunks for indexing
"""

import json
import os
import re
from datetime import datetime
from typing import Dict, List, Any, Optional
from pathlib import Path


class AASLDDataCleaner:
    """Cleans and normalizes AASLD guideline JSON data for RAG indexing"""
    
    # Common navigation/boilerplate patterns to remove
    NAVIGATION_PATTERNS = [
        r'AASLD PublicationsHepatologyLiver TransplantationHepatology CommunicationsClinical Liver Disease',
        r'Visit our other sites',
        r'Log inorRegister',
        r'Subscribe to journal',
        r'Get new issue alerts',
        r'AASLD Member\? Login here',
        r'Subscribe to eTOC',
        r'Enter your Email address:',
        r'Privacy Policy',
        r'Journal Logo',
        r'ArticlesArticlesAdvanced Search',
        r'Toggle navigation',
        r'BrowsingHistory',
        r'Back to Top',
        r'Never Miss an Issue',
        r'Get new journal Tables of Contents',
        r'Customer Service',
        r'Contact us at:',
        r'Submit a Service Request',
        r'Manage Cookie Preferences',
        r'Copyright.*American Association for the Study of Liver Diseases',
        r'Content use for text and data mining and artificial intelligence training is not permitted',
        r'Your PrivacyTo give you the best possible experience',
        r'Accept All Cookies',
        r'Privacy Preference Center',
        r'Strictly Necessary Cookies',
        r'Functional Cookies',
        r'Performance Cookies',
        r'Advertising Cookies',
        r'Flash Player.*required',
        r'Get Adobe Flash Player',
        r'Email to Colleague',
        r"Colleague's E-mail is Invalid",
        r'Your message has been successfully sent',
        r'Some error has occurred while processing your request',
        r'Export toEnd Note',
        r'ProciteReference Manager',
        r'Save my selection',
        r'DownloadPDF',
        r'CiteCopy',
        r'ShareEmailFacebookXLinkedIn',
        r'FavoritesPermissions',
        r'Related Articles',
        r'Readers Of this Article Also Read',
        r'Most Popular',
    ]
    
    # Patterns for extracting recommendations
    RECOMMENDATION_PATTERNS = [
        r'Recommendation\s+\d+',
        r'\(Strong recommendation[^)]+\)',
        r'\(Conditional recommendation[^)]+\)',
        r'\(Weak recommendation[^)]+\)',
        r'\(Moderate certainty\)',
        r'\(Low certainty\)',
        r'\(Very low certainty\)',
        r'\(High certainty\)',
    ]
    
    # Clinical value patterns (dosages, thresholds, etc.)
    CLINICAL_VALUE_PATTERNS = [
        r'\d+\s*(mg|mcg|IU/mL|U/L|IU|mL|kg|years?|months?|weeks?|days?)',
        r'≥\s*\d+',
        r'≤\s*\d+',
        r'<\s*\d+',
        r'>\s*\d+',
        r'\d+\s*-\s*\d+',
        r'\d+\s*to\s*\d+',
    ]
    
    def __init__(self, input_dir: str, output_dir: str = "cleaned_data"):
        self.input_dir = Path(input_dir)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
    def clean_text(self, text: str) -> str:
        """Clean and normalize text content"""
        if not text:
            return ""
        
        # Remove navigation/boilerplate patterns
        for pattern in self.NAVIGATION_PATTERNS:
            text = re.sub(pattern, '', text, flags=re.IGNORECASE)
        
        # Normalize whitespace
        text = re.sub(r'\s+', ' ', text)  # Multiple spaces to single
        text = re.sub(r'\n\s*\n+', '\n\n', text)  # Multiple newlines to double
        text = re.sub(r'[ \t]+', ' ', text)  # Tabs and multiple spaces
        
        # Fix common formatting issues
        text = re.sub(r'([a-z])([A-Z])', r'\1 \2', text)  # Add space between camelCase
        text = re.sub(r'([\.!?])([A-Z])', r'\1 \2', text)  # Space after sentence
        
        # Remove excessive punctuation
        text = re.sub(r'\.{3,}', '...', text)
        text = re.sub(r'-{3,}', '---', text)
        
        # Clean up copyright and boilerplate at end
        text = re.sub(r'Copyright.*$', '', text, flags=re.MULTILINE | re.DOTALL)
        
        return text.strip()
    
    def extract_recommendations(self, text: str) -> List[Dict[str, Any]]:
        """Extract recommendation statements with grades"""
        recommendations = []
        
        # Find recommendation blocks
        rec_pattern = r'Recommendation\s+(\d+)[:\s]+(.*?)(?=Recommendation\s+\d+|Case\s+\d+|$|Summary)'
        matches = re.finditer(rec_pattern, text, re.IGNORECASE | re.DOTALL)
        
        for match in matches:
            rec_num = match.group(1)
            rec_text = match.group(2).strip()
            
            # Extract recommendation grade
            grade_match = re.search(r'\((Strong|Conditional|Weak)\s+recommendation[^)]+\)', rec_text)
            grade = grade_match.group(1) if grade_match else None
            
            # Extract certainty
            certainty_match = re.search(r'\((?:high|moderate|low|very low)\s+certainty\)', rec_text, re.IGNORECASE)
            certainty = certainty_match.group(0) if certainty_match else None
            
            # Clean recommendation text
            rec_text = self.clean_text(rec_text)
            
            if rec_text:
                recommendations.append({
                    'number': rec_num,
                    'text': rec_text,
                    'grade': grade,
                    'certainty': certainty,
                    'raw_text': match.group(0)
                })
        
        return recommendations
    
    def extract_clinical_values(self, text: str) -> List[Dict[str, Any]]:
        """Extract clinical values (dosages, thresholds, etc.)"""
        values = []
        
        # Dosage patterns
        dosage_pattern = r'(\d+(?:\.\d+)?)\s*(mg|mcg|IU/mL|U/L|IU|mL|kg)\s*(?:orally|subcutaneously|daily|weekly|monthly)?'
        for match in re.finditer(dosage_pattern, text, re.IGNORECASE):
            values.append({
                'type': 'dosage',
                'value': match.group(0),
                'number': match.group(1),
                'unit': match.group(2)
            })
        
        # Threshold patterns
        threshold_pattern = r'(≥|≤|<|>|>=|<=)\s*(\d+(?:,\d+)?)\s*(IU/mL|U/L|IU|mg/dL|years?|months?|weeks?|days?)'
        for match in re.finditer(threshold_pattern, text, re.IGNORECASE):
            values.append({
                'type': 'threshold',
                'value': match.group(0),
                'operator': match.group(1),
                'number': match.group(2),
                'unit': match.group(3)
            })
        
        return values
    
    def clean_html_content(self, content: Dict[str, Any]) -> Dict[str, Any]:
        """Clean HTML content structure"""
        cleaned = {
            'full_text': self.clean_text(content.get('full_text', '')),
            'sections': [],
            'tables': [],
            'links': content.get('links', []),
            'recommendations': [],
            'clinical_values': []
        }
        
        # Clean sections
        if 'sections' in content and isinstance(content['sections'], list):
            for section in content['sections']:
                if isinstance(section, dict):
                    heading = self.clean_text(section.get('heading', ''))
                    # Skip navigation sections
                    if any(nav in heading.lower() for nav in ['logo', 'navigation', 'cookie', 'privacy']):
                        continue
                    
                    section_content = section.get('content', [])
                    if isinstance(section_content, list):
                        cleaned_content = [self.clean_text(str(item)) for item in section_content]
                        cleaned_content = [c for c in cleaned_content if len(c) > 10]  # Remove very short items
                        
                        if heading or cleaned_content:
                            cleaned['sections'].append({
                                'heading': heading,
                                'level': section.get('level', 1),
                                'content': cleaned_content
                            })
        
        # Clean tables
        if 'tables' in content and isinstance(content['tables'], list):
            for table in content['tables']:
                if isinstance(table, dict):
                    cleaned_table = {
                        'caption': self.clean_text(table.get('caption', '')),
                        'headers': [self.clean_text(str(h)) for h in table.get('headers', [])],
                        'rows': []
                    }
                    for row in table.get('rows', []):
                        if isinstance(row, list):
                            cleaned_row = [self.clean_text(str(cell)) for cell in row]
                            cleaned_table['rows'].append(cleaned_row)
                    cleaned['tables'].append(cleaned_table)
        
        # Extract recommendations and clinical values from full text
        cleaned['recommendations'] = self.extract_recommendations(cleaned['full_text'])
        cleaned['clinical_values'] = self.extract_clinical_values(cleaned['full_text'])
        
        # Preserve metadata
        cleaned['word_count'] = content.get('word_count', 0)
        cleaned['paragraph_count'] = content.get('paragraph_count', 0)
        cleaned['section_count'] = len(cleaned['sections'])
        cleaned['table_count'] = len(cleaned['tables'])
        
        return cleaned
    
    def clean_pdf_content(self, content: Dict[str, Any]) -> Dict[str, Any]:
        """Clean PDF content structure"""
        cleaned = {
            'full_text': self.clean_text(content.get('full_text', '')),
            'paragraphs': [],
            'recommendations': [],
            'clinical_values': []
        }
        
        # Clean paragraphs
        if 'paragraphs' in content and isinstance(content['paragraphs'], list):
            for para in content['paragraphs']:
                cleaned_para = self.clean_text(str(para))
                if len(cleaned_para) > 10:  # Remove very short paragraphs
                    cleaned['paragraphs'].append(cleaned_para)
        
        # Extract recommendations and clinical values
        cleaned['recommendations'] = self.extract_recommendations(cleaned['full_text'])
        cleaned['clinical_values'] = self.extract_clinical_values(cleaned['full_text'])
        
        # Preserve metadata
        cleaned['word_count'] = content.get('word_count', 0)
        cleaned['paragraph_count'] = len(cleaned['paragraphs'])
        cleaned['page_count'] = content.get('page_count', 0)
        
        return cleaned
    
    def clean_file(self, filepath: Path) -> Optional[Dict[str, Any]]:
        """Clean a single JSON file"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            cleaned_data = {
                'file_id': filepath.stem,
                'page_url': data.get('page_url', ''),
                'page_title': self.clean_text(data.get('page_title', '')),
                'content_type': data.get('content_type', ''),
                'crawled_at': data.get('crawled_at', ''),
                'accessible': data.get('accessible', False),
                'content': {}
            }
            
            # Clean content based on type
            if 'content' in data:
                if cleaned_data['content_type'] == 'html':
                    cleaned_data['content'] = self.clean_html_content(data['content'])
                elif cleaned_data['content_type'] == 'pdf':
                    cleaned_data['content'] = self.clean_pdf_content(data['content'])
                else:
                    # Fallback for unknown types
                    cleaned_data['content'] = {
                        'full_text': self.clean_text(data['content'].get('full_text', ''))
                    }
            
            # Add extraction date
            cleaned_data['cleaned_at'] = datetime.now().isoformat()
            
            return cleaned_data
            
        except Exception as e:
            print(f"Error cleaning {filepath}: {e}")
            return None
    
    def process_all_files(self) -> Dict[str, Any]:
        """Process all JSON files in the input directory"""
        json_files = list(self.input_dir.glob('*.json'))
        total_files = len(json_files)
        
        print(f"Processing {total_files} JSON files...")
        
        cleaned_files = []
        stats = {
            'total_files': total_files,
            'successful': 0,
            'failed': 0,
            'html_files': 0,
            'pdf_files': 0,
            'total_recommendations': 0,
            'total_clinical_values': 0,
            'total_words': 0
        }
        
        for i, filepath in enumerate(sorted(json_files), 1):
            print(f"Processing {i}/{total_files}: {filepath.name}")
            
            cleaned_data = self.clean_file(filepath)
            
            if cleaned_data:
                # Save cleaned file
                output_file = self.output_dir / f"{filepath.stem}_cleaned.json"
                with open(output_file, 'w', encoding='utf-8') as f:
                    json.dump(cleaned_data, f, indent=2, ensure_ascii=False)
                
                cleaned_files.append(cleaned_data)
                stats['successful'] += 1
                
                # Update stats
                if cleaned_data['content_type'] == 'html':
                    stats['html_files'] += 1
                elif cleaned_data['content_type'] == 'pdf':
                    stats['pdf_files'] += 1
                
                content = cleaned_data.get('content', {})
                stats['total_recommendations'] += len(content.get('recommendations', []))
                stats['total_clinical_values'] += len(content.get('clinical_values', []))
                stats['total_words'] += content.get('word_count', 0)
            else:
                stats['failed'] += 1
        
        # Save summary
        summary = {
            'cleaning_date': datetime.now().isoformat(),
            'statistics': stats,
            'files': [
                {
                    'file_id': f['file_id'],
                    'title': f['page_title'],
                    'type': f['content_type'],
                    'url': f['page_url'],
                    'recommendations_count': len(f.get('content', {}).get('recommendations', [])),
                    'clinical_values_count': len(f.get('content', {}).get('clinical_values', []))
                }
                for f in cleaned_files
            ]
        }
        
        summary_file = self.output_dir / 'cleaning_summary.json'
        with open(summary_file, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        
        print(f"\nCleaning complete!")
        print(f"  Successful: {stats['successful']}/{total_files}")
        print(f"  Failed: {stats['failed']}")
        print(f"  Total recommendations extracted: {stats['total_recommendations']}")
        print(f"  Total clinical values extracted: {stats['total_clinical_values']}")
        print(f"  Output directory: {self.output_dir}")
        
        return summary


def main():
    """Main execution function"""
    input_dir = Path(__file__).parent
    output_dir = input_dir / "cleaned_data"
    
    cleaner = AASLDDataCleaner(input_dir, output_dir)
    summary = cleaner.process_all_files()
    
    return summary


if __name__ == "__main__":
    main()

