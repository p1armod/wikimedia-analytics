package com.wikimedia.gateway.repository;

import com.wikimedia.gateway.model.EditStatsSnapshot;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.time.Instant;
import java.util.List;


@Repository
public interface EditStatsSnapshotRepository extends JpaRepository<EditStatsSnapshot, Long> {

    
    List<EditStatsSnapshot> findByRecordedAtBetweenOrderByRecordedAtAsc(Instant from, Instant to);

    
    EditStatsSnapshot findTopByOrderByRecordedAtDesc();

    
    List<EditStatsSnapshot> findTop20ByOrderByRecordedAtDesc();

    
    void deleteByRecordedAtBefore(Instant expiryDate);
}
