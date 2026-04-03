    def run_pipeline_batch(self, 
                          max_records: Optional[int] = None,
                          enable_semantic_validation: bool = True,
                          enable_reward_scoring: bool = True,
                          batch_write_size: int = 1000) -> Dict[str, Any]:
        """
        Run the complete Layer 5 validation pipeline with incremental I/O.
        
        Processes records in batches and writes results incrementally to avoid
        memory issues and provide visibility during long runs.
        
        Args:
            max_records: Maximum number of records to validate
            enable_semantic_validation: Whether to run semantic validation (Stage 2)
            enable_reward_scoring: Whether to run reward scoring (Stage 3)
            batch_write_size: Number of records to process before writing to disk
            
        Returns:
            Dictionary with validation results and statistics
        """
        
        logger.info("="*80)
        logger.info("Starting Layer 5 Validation Pipeline (BATCH MODE)")
        logger.info("="*80)
        logger.info(f"Incremental writes every {batch_write_size} records")
        logger.info(f"Semantic validation: {enable_semantic_validation}")
        logger.info(f"Reward scoring: {enable_reward_scoring}")
        
        self.stats.start_time = datetime.now().isoformat()
        self.writer.total_records = 0  # Will be set after loading data
        
        try:
            # Step 1: Load and merge data from all layers
            logger.info("\n[Step 1/6] Loading data from Layers 1-4...")
            complete_records = self._load_and_merge_layer_data(max_records)
            
            if not complete_records:
                logger.error("No complete records found for validation")
                return self._create_error_result("No complete records found")
            
            total_records = len(complete_records)
            self.writer.total_records = total_records
            logger.info(f"Loaded {total_records:,} complete records for validation")
            
            # Calculate number of processing batches
            processing_batch_size = 100  # Process 100 records at a time through pipeline
            processing_batches = [complete_records[i:i + processing_batch_size] 
                                 for i in range(0, total_records, processing_batch_size)]
            total_processing_batches = len(processing_batches)
            
            logger.info(f"Processing in {total_processing_batches} batches of {processing_batch_size} records")
            
            all_final_results = []
            batch_counter = 0
            
            # Process each batch
            for batch_idx, record_batch in enumerate(processing_batches):
                logger.info(f"\n--- Processing Batch {batch_idx + 1}/{total_processing_batches} "
                           f"(records {batch_idx * processing_batch_size + 1}-"
                           f"{min((batch_idx + 1) * processing_batch_size, total_records)}) ---")
                
                # Step 2: Run deterministic validation
                logger.debug(f"  Running deterministic validation...")
                deterministic_results = self._run_deterministic_validation(record_batch)
                
                # Step 3: Run semantic validation
                semantic_results = {}
                if enable_semantic_validation:
                    logger.debug(f"  Running semantic validation...")
                    semantic_results = self._run_semantic_validation(record_batch)
                
                # Step 4: Run reward scoring
                reward_results = {}
                if enable_reward_scoring:
                    logger.debug(f"  Running reward scoring...")
                    reward_results = self._run_reward_scoring(record_batch)
                
                # Step 5: Run statistical validation
                logger.debug(f"  Running statistical validation...")
                statistical_results = self._run_statistical_validation(record_batch)
                
                # Step 6: Combine results for this batch
                logger.debug(f"  Combining results...")
                batch_final_results = self._combine_validation_results(
                    record_batch,
                    deterministic_results,
                    semantic_results,
                    reward_results,
                    statistical_results
                )
                
                # Add to overall results
                all_final_results.extend(batch_final_results)
                batch_counter += len(batch_final_results)
                
                # Incremental write every batch_write_size records
                if batch_counter >= batch_write_size or (batch_idx + 1) == total_processing_batches:
                    logger.info(f"\n>>> Writing batch {batch_counter} records to disk...")
                    
                    # Write this batch to temp files
                    self.writer.write_batch(batch_final_results, batch_idx + 1)
                    
                    # Reset counter
                    batch_counter = 0
                    
                    # Force flush to disk
                    # (file handles are closed automatically in write_batch)
            
            # All records processed, now merge final outputs
            logger.info("\n" + "="*80)
            logger.info("[Step 6/6] Merging all temp files into final outputs...")
            logger.info("="*80)
            
            # Generate summary
            summary_report = self._generate_validation_summary(all_final_results)
            
            # Merge temp files into final CSVs
            output_files = self.writer.merge_final_outputs(summary_report)
            
            self.stats.end_time = datetime.now().isoformat()
            
            # Create final result
            result = self._create_success_result(all_final_results, output_files, summary_report)
            
            logger.info("\n" + "="*80)
            logger.info("Layer 5 validation pipeline completed successfully!")
            logger.info("="*80)
            
            return result
            
        except Exception as e:
            logger.error(f"Validation pipeline failed: {e}")
            self.stats.end_time = datetime.now().isoformat()
            
            # Try to save partial results if we have any
            if hasattr(self.writer, 'write_counter') and self.writer.write_counter > 0:
                logger.warning(f"Attempting to save partial results ({self.writer.write_counter} records)")
                try:
                    partial_summary = self._generate_validation_summary([])
                    self.writer.merge_final_outputs(partial_summary)
                except Exception as merge_error:
                    logger.error(f"Failed to save partial results: {merge_error}")
            
            return self._create_error_result(str(e))
