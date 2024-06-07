import CKRecordCoder
import CloudKit
import CloudKitCodable
import Foundation
import os.log

extension SyncEngine {
  
  // MARK: - Public
  
  /// Fetch remote changes
  /// - Parameter onCompletion: pass a completion block that gets called only after changes have beel pulled, emitted and consumed
  func fetchRemoteChanges(onCompletion: ((Result<SyncEngine<Model>.ModelChanges, Error>) -> Void)? = nil) {
    logHandler("\(#function)", .debug)

    // Dictionary to hold the latest version of each changed record
    var latestChangedRecords = [CKRecord.ID: CKRecord]()
    var deletedRecordIDs: [CKRecord.ID] = []
    
    // Completion handler invocation guard
    let useCompletion = onCompletion != nil
    var completionCalled = false
    let callCompletion: (Result<SyncEngine<Model>.ModelChanges, Error>) -> Void = { result in
      if !completionCalled {
        completionCalled = true
        onCompletion?(result)
      }
    }
    
    let operation = CKFetchRecordZoneChangesOperation()

    let config = CKFetchRecordZoneChangesOperation.ZoneConfiguration(
      previousServerChangeToken: privateChangeToken,
      resultsLimit: nil,
      desiredKeys: nil
    )

    operation.configurationsByRecordZoneID = [zoneIdentifier: config]

    operation.recordZoneIDs = [zoneIdentifier]
    operation.fetchAllChanges = true

    // Take individually changed record
    operation.recordChangedBlock = { [weak self] record in
      guard let self else { return }
      
      self.workQueue.async {
        latestChangedRecords[record.recordID] = record

        self.logHandler("... fetched changed item", .debug)
      }
    }

    // Take individually deleted record
    operation.recordWithIDWasDeletedBlock = { [weak self] recordID, recordType in
      guard let self else { return }
      
      self.workQueue.async { [weak self] in
        guard let self else { return }
        
        guard self.recordType == recordType else { return }

        self.logHandler("... fetched deleted item", .debug)

        deletedRecordIDs.append(recordID)
      }
    }
    
    // Called after the record zone fetch completes
    operation.recordZoneFetchCompletionBlock = { [weak self] _, newChangeToken, _, isFinalChange, error in
      guard let self else { return }
      
      if let error = error as? CKError {
        self.logHandler("Failed to fetch record zone changes (final? \(isFinalChange ? "yes" : "no")): \(String(describing: error))", .error)
        
        // Clear accumulated changes as we'll be trying again
        latestChangedRecords.removeAll()
        deletedRecordIDs.removeAll()
        
        if error.code == .changeTokenExpired {
          self.workQueue.async { [weak self] in
            guard let self else { return }

            self.logHandler("Change token expired, resetting token and trying again", .error)
            
            self.privateChangeToken = nil
            self.fetchRemoteChanges(onCompletion: callCompletion)
          }
        }
        else {
          let result = error.retryCloudKitOperationIfPossible(self.logHandler, queue: self.workQueue) {
            self.fetchRemoteChanges(onCompletion: callCompletion)
          }
          
          if !result {
            logHandler("Fetch is not possible: \( String(describing: error))", .error)
            
            self.workQueue.async {
              callCompletion(.failure(error))
            }
          }
        }
      }
      else {
        self.workQueue.async { [weak self] in
          guard let self else { return }

          let changedRecords = Array(latestChangedRecords.values)
          
          if deletedRecordIDs.isEmpty && latestChangedRecords.isEmpty {
            self.logHandler("Fetch completed [\(changedRecords.count) changes, \(deletedRecordIDs.count) deletes]", .info)
          }
          else {
            self.logHandler("Finalizing fetch [\(changedRecords.count) changes, \(deletedRecordIDs.count) deletes]", .info)
          }
          
          if useCompletion {
            // When using a completion block, accumulated changes are returned at once
            callCompletion(.success(makeModelChanges(with: changedRecords,
                                                     deletedRecordIDs: deletedRecordIDs,
                                                     andChangeToken: newChangeToken)))
          }
          else {
            self.emitServerChanges(with: changedRecords,
                                   deletedRecordIDs: deletedRecordIDs,
                                   andChangeToken: newChangeToken)
            
          }
          
          latestChangedRecords.removeAll()
          deletedRecordIDs.removeAll()
        }
      }
    }


    // Called after the entire fetch operation completes for all zones
    operation.fetchRecordZoneChangesCompletionBlock = { [weak self] error in
      guard let self else { return }

      if let error {
        self.logHandler("Failed to fetch record zone changes: \(String(describing: error))", .error)

        let result = error.retryCloudKitOperationIfPossible(self.logHandler, queue: self.workQueue) {
          self.fetchRemoteChanges(onCompletion: callCompletion)
        }
        
        if !result {
          logHandler("Fetch operation is not possible: \( String(describing: error))", .error)
          
          self.workQueue.async {
            callCompletion(.failure(error))
          }
        }
      }
      else {
        self.logHandler("Finished fetching record zone changes", .info)
        
        // DO NOT CALL COMPLETION HERE - Upon a successful fetch we expect
        // recordZoneFetchCompletionBlock to have completed with the appropriate
        // change-token and final changes.
      }
    }

    operation.qualityOfService = .userInitiated
    operation.database = privateDatabase

    cloudOperationQueue.addOperation(operation)
    
    // we want to wait for the fetch to complete before pushing changes
    if !useCompletion {
      cloudOperationQueue.waitUntilAllOperationsAreFinished()
    }
  }

  // MARK: - Private

  internal var privateChangeToken: CKServerChangeToken? {
    get {
      guard let data = defaults.data(forKey: privateChangeTokenKey) else { return nil }
      guard !data.isEmpty else { return nil }

      do {
        let token = try NSKeyedUnarchiver.unarchivedObject(
          ofClass: CKServerChangeToken.self, from: data)

        return token
      } catch {
        logHandler(
          "Failed to decode CKServerChangeToken from defaults key privateChangeToken", .error)
        return nil
      }
    }
    set {
      guard let newValue = newValue else {
        defaults.setValue(Data(), forKey: privateChangeTokenKey)
        return
      }

      do {
        let data = try NSKeyedArchiver.archivedData(
          withRootObject: newValue, requiringSecureCoding: true)

        defaults.set(data, forKey: privateChangeTokenKey)
      } catch {
        logHandler(
          "Failed to encode private change token: \(String(describing: error))", .error)
      }
    }
  }
  
  // MARK: - Helper

  private func emitServerChanges(with changedRecords: [CKRecord], 
                                 deletedRecordIDs: [CKRecord.ID],
                                 andChangeToken changeToken: CKServerChangeToken?) {
    guard !changedRecords.isEmpty || !deletedRecordIDs.isEmpty else {
      logHandler("Finished record zone changes fetch with no changes", .info)
      return
    }

    let modelChanges = makeModelChanges(with: changedRecords, deletedRecordIDs: deletedRecordIDs, andChangeToken: changeToken)

    modelsChangedSubject.send(modelChanges)
  }
  
  /// Wrap changes that need to be returned to the caller
  /// - Parameters:
  ///   - changedRecords: changed records
  ///   - deletedRecordIDs: deleted record IDs
  ///   - changeToken: the new change token for these set of changes. Passed back to the consumer for it to update the SyncEngine after processing changes.
  private func makeModelChanges(with changedRecords: [CKRecord],
                                deletedRecordIDs: [CKRecord.ID],
                                andChangeToken changeToken: CKServerChangeToken? = nil) -> ModelChanges {
    guard !changedRecords.isEmpty || !deletedRecordIDs.isEmpty else {
      logHandler("Finished record zone changes fetch with no changes", .info)
      return .init()
    }
    
    logHandler("Fetched \(changedRecords.count) changed record(s) and \(deletedRecordIDs.count) deleted record(s)", .info)
    
    let models: Set<Model> = Set(
      changedRecords.compactMap { record in
        do {
          let decoder = CKRecordDecoder()
          return try decoder.decode(Model.self, from: record)
        } catch {
          logHandler(
            "Error decoding item from record: \(String(describing: error))", .error)
          return nil
        }
      })
    
    let deletedIdentifiers = Set(deletedRecordIDs.map(\.recordName))
    
    return .init(updates: .updatesPulled(models),
                 deletes: .deletesPulled(deletedIdentifiers),
                 changeToken: changeToken)
  }
}
