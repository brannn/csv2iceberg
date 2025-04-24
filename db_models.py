"""
Database models for storing job information
"""
import os
import json
import datetime
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import JSON

# Initialize SQLAlchemy
db = SQLAlchemy()

class Job(db.Model):
    """Job model for storing conversion job data"""
    __tablename__ = 'jobs'
    
    id = db.Column(db.String(64), primary_key=True)
    status = db.Column(db.String(16), nullable=False, default='pending')
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.datetime.utcnow)
    started_at = db.Column(db.DateTime, nullable=True)
    completed_at = db.Column(db.DateTime, nullable=True)
    progress = db.Column(db.Integer, default=0)
    is_test = db.Column(db.Boolean, default=False)
    filename = db.Column(db.String(255), nullable=True)
    file_path = db.Column(db.String(512), nullable=True)
    params = db.Column(JSON, nullable=True)
    stdout = db.Column(db.Text, nullable=True)
    stderr = db.Column(db.Text, nullable=True)
    returncode = db.Column(db.Integer, nullable=True)
    error = db.Column(db.Text, nullable=True)
    last_viewed_at = db.Column(db.DateTime, nullable=True)
    
    def to_dict(self):
        """Convert job model to dictionary representation"""
        job_dict = {
            'id': self.id,
            'status': self.status,
            'progress': self.progress,
            'is_test': self.is_test,
            'filename': self.filename,
            'file_path': self.file_path,
            'params': self.params or {},
            'stdout': self.stdout,
            'stderr': self.stderr,
            'returncode': self.returncode,
            'error': self.error
        }
        
        # Handle datetime fields
        if self.created_at:
            job_dict['created_at'] = self.created_at
        if self.started_at:
            job_dict['started_at'] = self.started_at
        if self.completed_at:
            job_dict['completed_at'] = self.completed_at
        if self.last_viewed_at:
            job_dict['last_viewed_at'] = self.last_viewed_at
            
        return job_dict
        
    @classmethod
    def from_dict(cls, job_dict):
        """Create a job model from dictionary data"""
        # Extract fields from the dictionary to create a new job
        job = cls(
            id=job_dict.get('id'),
            status=job_dict.get('status', 'pending'),
            progress=job_dict.get('progress', 0),
            is_test=job_dict.get('is_test', False),
            filename=job_dict.get('filename'),
            file_path=job_dict.get('file_path'),
            params=job_dict.get('params', {}),
            stdout=job_dict.get('stdout'),
            stderr=job_dict.get('stderr'),
            returncode=job_dict.get('returncode'),
            error=job_dict.get('error')
        )
        
        # Handle datetime fields
        if 'created_at' in job_dict and job_dict['created_at']:
            if isinstance(job_dict['created_at'], str):
                job.created_at = datetime.datetime.fromisoformat(job_dict['created_at'])
            else:
                job.created_at = job_dict['created_at']
        else:
            job.created_at = datetime.datetime.utcnow()
            
        if 'started_at' in job_dict and job_dict['started_at']:
            if isinstance(job_dict['started_at'], str):
                job.started_at = datetime.datetime.fromisoformat(job_dict['started_at'])
            else:
                job.started_at = job_dict['started_at']
                
        if 'completed_at' in job_dict and job_dict['completed_at']:
            if isinstance(job_dict['completed_at'], str):
                job.completed_at = datetime.datetime.fromisoformat(job_dict['completed_at'])
            else:
                job.completed_at = job_dict['completed_at']
                
        if 'last_viewed_at' in job_dict and job_dict['last_viewed_at']:
            if isinstance(job_dict['last_viewed_at'], str):
                job.last_viewed_at = datetime.datetime.fromisoformat(job_dict['last_viewed_at'])
            else:
                job.last_viewed_at = job_dict['last_viewed_at']
                
        return job