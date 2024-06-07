import CloudKit
import Foundation
import os.log

extension SyncEngine {

  // MARK: - Internal

  func initializeZone() async throws -> Bool {
    try await withCheckedThrowingContinuation { continuation in
      createCustomZoneIfNeeded { result in
        switch result {
          case .success(let success):
            continuation.resume(returning: success)
          case .failure(let error):
            continuation.resume(throwing: error)
        }
      }
    }
  }

  // MARK: - Private

  private var createdCustomZone: Bool {
    get {
      return defaults.bool(forKey: createdCustomZoneKey) || defaults.bool(forKey: legacyCreatedCustomZoneKey)
    }
    set {
      defaults.set(newValue, forKey: createdCustomZoneKey)
      defaults.removeObject(forKey: legacyCreatedCustomZoneKey)
    }
  }

  private func createCustomZoneIfNeeded(isRetrying: Bool = false, onCompletion: @escaping ((Result<Bool, Error>) -> Void)) {
    guard !createdCustomZone else {
      logHandler("Already have custom zone, skipping creation but checking if zone really exists", .debug)

      checkCustomZone(isRetrying: isRetrying, onCompletion: onCompletion)
        
      return
    }

    logHandler("Creating CloudKit zone \(zoneIdentifier.zoneName)", .info)

    let zone = CKRecordZone(zoneID: zoneIdentifier)
    let operation = CKModifyRecordZonesOperation(
      recordZonesToSave: [zone],
      recordZoneIDsToDelete: nil
    )

    operation.modifyRecordZonesCompletionBlock = { [weak self] _, _, error in
      guard let self else { return }

      if let error {
        self.logHandler("Failed to create custom CloudKit zone, trying again: \(String(describing: error))", .error)

        var result = false
        
        // Don't try a second time
        if !isRetrying {
          result = error.retryCloudKitOperationIfPossible(self.logHandler, queue: self.workQueue) {
            self.createCustomZoneIfNeeded(isRetrying: true, onCompletion: onCompletion)
          }
        }
        
        if result == false {
          onCompletion(.failure(error))
        }
      }
      else {
        self.logHandler("Zone created successfully", .info)
        self.createdCustomZone = true
        
        onCompletion(.success(true))
      }
    }

    operation.qualityOfService = .userInitiated
    operation.database = privateDatabase

    cloudOperationQueue.addOperation(operation)
  }

  private func checkCustomZone(isRetrying: Bool = false, onCompletion: @escaping ((Result<Bool, Error>) -> Void)) {
    let operation = CKFetchRecordZonesOperation(recordZoneIDs: [zoneIdentifier])

    operation.fetchRecordZonesCompletionBlock = { [weak self] ids, error in
      guard let self else { return }

      if let error {
        self.logHandler("Failed to check for custom zone existence: \(String(describing: error))", .error)

        var result = false
        
        if !isRetrying {
          result = error.retryCloudKitOperationIfPossible(self.logHandler, queue: self.workQueue) {
            self.checkCustomZone(onCompletion: onCompletion)
          }
        }
        
        if isRetrying || result == false {
          self.logHandler("Irrecoverable error when fetching custom zone, assuming it doesn't exist: \(String(describing: error))", .error)
          self.workQueue.async { [weak self] in
            guard let self else { return }
            
            self.createdCustomZone = false
            self.createCustomZoneIfNeeded(onCompletion: onCompletion)
          }
        }
      } 
      else if ids?.isEmpty ?? true {
        self.logHandler("Custom zone reported as existing, but it doesn't exist. Creating.", .error)
        
        self.workQueue.async { [weak self] in
          guard let self else { return }
          
          self.createdCustomZone = false
          self.createCustomZoneIfNeeded(onCompletion: onCompletion)
        }
      }
      else {
        self.workQueue.async { [weak self] in
          guard let self else { return }
          
          // If checking failed earlier, we set this flag to false, must reset to true if zone in fact exists, otherwise we'll keep
          // trying to create the zone, which will always throw an error
          if self.createdCustomZone == false {
            self.createdCustomZone = true
            
            self.logHandler("Custom zone exists, updating flag", .debug)
          }
          
          onCompletion(.success(true))
        }
      }
    }

    operation.qualityOfService = .userInitiated
    operation.database = privateDatabase

    cloudOperationQueue.addOperation(operation)
  }
}
