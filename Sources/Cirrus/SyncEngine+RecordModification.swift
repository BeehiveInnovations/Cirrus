import CloudKit
import Foundation
import os.log

extension SyncEngine {
  
  // MARK: - Internal
  
  func saveRecord(_ model: Model,
                    usingContext context: RecordModifyingContextProvider,
                    onCompletion: @escaping ((Result<SyncEngine<Model>.ModelChanges, Error>) -> Void)) {
    guard let record = try? context.encodedRecord(model) else {
      onCompletion(.failure(CKError(.invalidArguments)))
      return
    }
    
    modifyRecords(toSave: [record],
                  recordIDsToDelete: [],
                  context: context, 
                  onCompletion: onCompletion)
  }
  
  func deleteRecord(_ model: Model,
                    usingContext context: RecordModifyingContextProvider,
                    onCompletion: @escaping ((Result<SyncEngine<Model>.ModelChanges, Error>) -> Void)) {
    guard let record = try? context.encodedRecord(model) else {
      onCompletion(.failure(CKError(.invalidArguments)))
      return
    }
    
    modifyRecords(toSave: [],
                  recordIDsToDelete: [record.recordID],
                  context: context,
                  onCompletion: onCompletion)
  }
  
  // MARK: - Private
  
  private func modifyRecords(toSave recordsToSave: [CKRecord],
                             recordIDsToDelete: [CKRecord.ID],
                             context: RecordModifyingContextProvider,
                             savePolicyOverride: CKModifyRecordsOperation.RecordSavePolicy? = nil,
                             onCompletion: @escaping ((Result<SyncEngine<Model>.ModelChanges, Error>) -> Void)) {
    guard !recordIDsToDelete.isEmpty || !recordsToSave.isEmpty else {
      onCompletion(.success(.init()))
      return
    }
    
    if recordsToSave.isEmpty {
      logHandler("Sending \(recordIDsToDelete.count) record(s) for deletion", .debug)
    }
    else {
      logHandler("Sending \(recordsToSave.count) record(s) for upload", .debug)
    }
    
    let operation = CKModifyRecordsOperation(recordsToSave: recordsToSave.isEmpty ? nil : recordsToSave,
                                             recordIDsToDelete: recordIDsToDelete.isEmpty ? nil : recordIDsToDelete)
    
    operation.modifyRecordsCompletionBlock = { [weak self] serverRecords, deletedRecordIDs, error in
      guard let self else {
        onCompletion(.failure(CKError(.internalError)))
        return
      }
      
      if let error {
        self.logHandler("Failed to \(context.name) records: \(String(describing: error))", .error)
        
        self.workQueue.async { [weak self] in
          guard let self else {
            onCompletion(.failure(CKError(.internalError)))
            return
          }
          
          self.handleError(error,
                           toSave: recordsToSave,
                           recordIDsToDelete: recordIDsToDelete,
                           context: context,
                           onCompletion: onCompletion)
        }
      }
      else {
        if !(deletedRecordIDs?.isEmpty ?? true), recordsToSave.isEmpty {
          self.logHandler("Successfully \(context.name) record(s). Deleted \(deletedRecordIDs?.count ?? 0)", .info)
        }
        else if (deletedRecordIDs?.isEmpty ?? true), !(serverRecords?.isEmpty ?? true) {
          self.logHandler("Successfully \(context.name) record(s). Saved \(serverRecords?.count ?? 0)", .info)
        }
        else {
          self.logHandler("Successfully \(context.name) record(s). Saved \(serverRecords?.count ?? 0) and deleted \(deletedRecordIDs?.count ?? 0)", .info)
        }
        
        self.workQueue.async {
          self.lastChangeDate = Date()
          
          let modelChanges: SyncEngine<Model>.ModelChanges = context.modelChangeForUpdatedRecords(recordsSaved: serverRecords ?? [],
                                                                                                  recordIDsDeleted: deletedRecordIDs ?? [])
          
          onCompletion(.success(modelChanges))
        }
      }
    }
    
    operation.savePolicy = savePolicyOverride ?? self.savePolicy
    operation.database = privateDatabase
    
    cloudOperationQueue.addOperation(operation)
  }
  
  // MARK: - Private
  
  private func handleError(_ error: Error,
                           toSave recordsToSave: [CKRecord],
                           recordIDsToDelete: [CKRecord.ID],
                           context: RecordModifyingContextProvider,
                           onCompletion: @escaping ((Result<SyncEngine<Model>.ModelChanges, Error>) -> Void)) {
    guard let ckError = error as? CKError else {
      logHandler( "Error was not a CKError, giving up: \(String(describing: error))", .fault)
      onCompletion(.failure(error))
      return
    }
    
    switch ckError {
      case _ where ckError.isCloudKitZoneDeleted:
        // If the CloudKit zone is deleted, the code logs this and attempts to recreate the zone. After recreating, it retries the modify operation.
        
        logHandler("Zone was deleted: \(String(describing: error))", .error)
        
        onCompletion(.failure(error))
      case _ where ckError.code == CKError.Code.limitExceeded:
        // The operation splits the record saves and deletions into smaller chunks and retries them separately. 
        // This is done to comply with CloudKit's limitations on batch sizes.
        
        logHandler("CloudKit batch limit exceeded, trying to \(context.name) records in chunks", .error)
        
        let firstHalfSave = recordsToSave.count <= 1 ? recordsToSave : Array(recordsToSave[0..<recordsToSave.count / 2])
        let secondHalfSave = recordsToSave.count <= 1 ? [] : Array(recordsToSave[recordsToSave.count / 2..<recordsToSave.count])
        
        let firstHalfDelete = recordIDsToDelete.count <= 1 ? recordIDsToDelete : Array(recordIDsToDelete[0..<recordIDsToDelete.count / 2])
        let secondHalfDelete = recordIDsToDelete.count <= 1 ? [] : Array(recordIDsToDelete[recordIDsToDelete.count / 2..<recordIDsToDelete.count])
        
        let results = [(firstHalfSave, firstHalfDelete), (secondHalfSave, secondHalfDelete)].map {
          (save: [CKRecord], delete: [CKRecord.ID]) in
          
          error.retryCloudKitOperationIfPossible(self.logHandler, queue: self.workQueue) {
            self.modifyRecords(
              toSave: save,
              recordIDsToDelete: delete,
              context: context,
              onCompletion: onCompletion
            )
          }
        }
        
        if !results.allSatisfy({ $0 == true }) {
          logHandler("Error is not recoverable: \(String(describing: error))", .error)
          
          onCompletion(.failure(error))
        }
        
      case _ where ckError.code == .partialFailure:
        // Even if there was one item to save, a partial failure can be returned. Must be handled here
        
        if let partialErrors = ckError.userInfo[CKPartialErrorsByItemIDKey] as? [CKRecord.ID: Error] {
          let recordIDsNotSavedOrDeleted = Set(partialErrors.keys)

          // An error that occurs when the system rejects the entire batch of changes.
          let batchRequestFailedRecordIDs = Set(
            partialErrors.filter({ (_, error) in
              if let error = error as? CKError,
                 error.code == .batchRequestFailed
              {
                return true
              }
              return false
            }).keys)
          
          // An error that occurs when CloudKit rejects a record because the server’s version is different.
          let serverRecordChangedErrors = partialErrors.filter({ (_, error) in
            if let error = error as? CKError,
               error.code == .serverRecordChanged
            {
              return true
            }
            return false
          }).values
          
          // An error that occurs when the specified record doesn’t exist.
          let unknownItemRecordIDs = Set(
            partialErrors.filter({ (_, error) in
              if let error = error as? CKError,
                 error.code == .unknownItem
              {
                return true
              }
              return false
            }).keys)
          
          let unknownRecords = recordsToSave.filter { unknownItemRecordIDs.contains($0.recordID) }
          let unknownDeletedIDs = recordIDsToDelete.filter(unknownItemRecordIDs.contains)
          
          let recordsToSaveWithoutUnknowns =
          recordsToSave
            .filter { recordIDsNotSavedOrDeleted.contains($0.recordID) }
            .filter { !unknownItemRecordIDs.contains($0.recordID) }
          
          let recordIDsToDeleteWithoutUnknowns =
          recordIDsToDelete
            .filter(recordIDsNotSavedOrDeleted.contains)
            .filter { !unknownItemRecordIDs.contains($0) }
          
          let resolvedConflictsToSave = serverRecordChangedErrors.compactMap {
            $0.resolveConflict(logHandler, with: Model.resolveConflict)
          }
          
          let conflictsToSaveSet = Set(resolvedConflictsToSave.map(\.recordID))
          
          let batchRequestFailureRecordsToSave = recordsToSaveWithoutUnknowns.filter {
            !conflictsToSaveSet.contains($0.recordID)
            && batchRequestFailedRecordIDs.contains($0.recordID)
          }
                    
          // Handle completion failures separately as these would be for individual items
          // If an unknown record could not be saved or deleted, stop and invoke the callback
          if !unknownRecords.isEmpty || !unknownDeletedIDs.isEmpty {
            // Let the caller decide what to do with these
            // This isn't considered a typical error but instead returned as success with
            // an known item. The caller may not care or may wish to delete the local copy
            let failedChanges: SyncEngine<Model>.ModelChanges = context.failedToUpdateRecords(recordsSaved: unknownRecords,
                                                                                              recordIDsDeleted: unknownDeletedIDs)
            
            onCompletion(.success(failedChanges))
          }
          else {
            let finalSavesWithoutUknowns = batchRequestFailureRecordsToSave + resolvedConflictsToSave
            
            modifyRecords(
              toSave: finalSavesWithoutUknowns,
              recordIDsToDelete: recordIDsToDeleteWithoutUnknowns,
              context: context,
              savePolicyOverride: .changedKeys,
              onCompletion: onCompletion
            )
          }
        }
        else {
          onCompletion(.failure(error))
        }
        
      case _ where ckError.code == .serverRecordChanged:
        if let resolvedRecord = error.resolveConflict(logHandler, with: Model.resolveConflict) {
          logHandler("Conflict resolved, will retry upload", .info)
          
          // Retry sending
          self.modifyRecords(toSave: [resolvedRecord],
                             recordIDsToDelete: [],
                             context: context,
                             savePolicyOverride: .changedKeys,
                             onCompletion: onCompletion)
        }
        else {
          logHandler("Resolving conflict returned a nil record. Giving up.", .error)
          
          onCompletion(.failure(error))
        }
        
      case _
        where ckError.code == .serviceUnavailable
        || ckError.code == .networkUnavailable
        || ckError.code == .networkFailure
        || ckError.code == .serverResponseLost:
        
        logHandler("Unable to connect to iCloud servers: \(String(describing: error))", .info)
        
        onCompletion(.failure(error))
        
      case _ where ckError.code == .unknownItem:
        logHandler("Unknown item, ignoring: \(String(describing: error))", .info)
        
        // Let the caller decide what to do with these
        // This isn't considered a typical error but instead returned as success with
        // an known item. The caller may not care or may wish to delete the local copy
        let failedChanges: SyncEngine<Model>.ModelChanges = context.failedToUpdateRecords(recordsSaved: recordsToSave,
                                                                                          recordIDsDeleted: recordIDsToDelete)
        
        onCompletion(.success(failedChanges))
        
      default:
        // Retry
        logHandler("Retrying for error: \(String(describing: error))", .info)
        
        let result = error.retryCloudKitOperationIfPossible(self.logHandler, queue: self.workQueue) {
          self.modifyRecords(toSave: recordsToSave,
                             recordIDsToDelete: recordIDsToDelete,
                             context: context,
                             onCompletion: onCompletion)
        }
        
        if !result {
          logHandler("Error is not recoverable: \( String(describing: error))", .error)
          
          let failedChanges: SyncEngine<Model>.ModelChanges = context.failedToUpdateRecords(recordsSaved: recordsToSave,
                                                                                            recordIDsDeleted: recordIDsToDelete)
          
          onCompletion(.success(failedChanges))
        }
    }
  }
}
