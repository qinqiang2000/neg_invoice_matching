# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Common Commands

### Environment Setup
```bash
# Activate virtual environment
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### Running Tests

```bash
# Generate test data；测试数据生成，都要用这个工具；如果工具缺失功能，也在这个工具中补全，避免重复“造轮子”
python tests/test_data_generator.py

# Basic matching tests
python tests/test_basic_matching.py

# Performance scaling tests
python tests/test_performance_scale.py

# Edge case tests
python tests/test_edge_cases.py
```

### Database Setup
Ensure PostgreSQL is running and update configuration in `config/config.py` with proper database credentials.

## Architecture Overview

This is a **negative invoice matching system** that implements a greedy algorithm to match negative invoices with blue invoice line items. The system is designed for high performance with PostgreSQL database integration.

### Core Components

#### 1. Matching Engine (`core/matching_engine.py`)
- **GreedyMatchingEngine**: Main algorithm implementation using greedy allocation strategy
- **Data Models**: BlueLineItem, NegativeInvoice, MatchResult, MatchAllocation
- **Batch Processing**: Supports both standard and streaming batch processing for large datasets
- **Grouping Strategy**: Groups negatives by (tax_rate, buyer_id, seller_id) to minimize database queries
- **Sorting Strategies**: amount_desc, amount_asc, priority_desc for different matching priorities

#### 2. Database Management (`core/db_manager.py`)
- **DatabaseManager**: Connection pooling, candidate retrieval, and result persistence
- **CandidateProvider**: Interface for matching engine to access database candidates
- **Transaction Management**: Atomic operations with conflict detection and rollback
- **Bulk Operations**: Optimized batch updates using PostgreSQL-specific features

#### 3. Performance Monitoring (`core/performance_monitor.py`)
- **PerformanceTimer**: Detailed timing for all operations with context managers
- **Resource Monitoring**: Memory and CPU usage tracking during execution
- **Database Statistics**: Query performance and table access patterns
- **Report Generation**: JSON export and console summaries

#### 4. Configuration (`config/config.py`)
- Database configurations for test/dev/prod environments
- Test scenario configurations (small, mixed, stress)
- Performance targets and thresholds

### Key Design Patterns

1. **Candidate Provider Pattern**: Decouples matching algorithm from data access
2. **Batch Grouping**: Reduces database round-trips by grouping negatives with same conditions
3. **Streaming Processing**: Handles large datasets without memory overflow
4. **Performance Monitoring**: Comprehensive instrumentation for optimization

### Data Flow

1. **Input**: List of negative invoices to be matched
2. **Grouping**: Group by (tax_rate, buyer_id, seller_id) to minimize queries
3. **Candidate Retrieval**: Fetch available blue line items for each group
4. **Matching**: Apply greedy algorithm within each group
5. **Result Persistence**: Atomic database updates with conflict detection
6. **Monitoring**: Performance metrics and resource usage tracking

### Database Schema Dependencies

The system expects these main tables:
- `blue_lines`: Contains line items with remaining amounts
- `match_records`: Stores matching results
- `negative_invoices`: Source negative invoice data

### Performance Characteristics

- **Small batches** (< 1,000): Standard processing
- **Large batches** (≥ 10,000): Automatic streaming processing
- **Memory usage**: Constant for streaming, linear for standard
- **Database optimization**: Bulk operations and connection pooling

### Testing Strategy

The test suite covers:
- Basic matching functionality and edge cases
- Performance scaling from hundreds to tens of thousands of records
- Data generation for various scenarios
- Algorithm improvements and optimizations